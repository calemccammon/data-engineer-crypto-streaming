"""
Kafka → Bronze consumer.

Reads trade events from the `raw-trades` Kafka topic and writes them to:
  1. DuckDB `raw.raw_trades` table in data/bronze.duckdb (append-only, deduped by symbol and trade_id)
  2. Rolling Parquet snapshots in data/bronze/ (every PARQUET_FLUSH_ROWS rows)

Bronze is the landing zone — data is written as-is with minimal transformation.
The only guarantee the DuckDB table makes is uniqueness on (symbol, trade_id).

Delivery is at-least-once. Kafka offsets are committed only after the trades
they cover are in a Parquet file, so a consumer that is killed re-reads what
it had not yet flushed. The Parquet files can therefore repeat a trade. The
Silver processor collapses those on read.

Usage:
    uv run python -m consume.bronze_consumer
"""

import json
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv
from pydantic import ValidationError

from ingest.schemas import TradeEvent

load_dotenv()

log = structlog.get_logger()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW_TRADES", "raw-trades")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "crypto-pipeline")
DUCKDB_PATH = os.getenv("BRONZE_DUCKDB_PATH", "data/bronze.duckdb")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
BRONZE_DIR = DATA_DIR / "bronze"

# Write a Parquet snapshot every N rows (configurable, default 5000)
PARQUET_FLUSH_ROWS = int(os.getenv("PARQUET_FLUSH_ROWS", "5000"))

# Parquet schema for the Bronze snapshots. Matches TradeEvent.
BRONZE_SCHEMA = pa.schema(
    [
        pa.field("trade_id", pa.int64()),
        pa.field("symbol", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("quantity", pa.float64()),
        pa.field("trade_time", pa.timestamp("us", tz="UTC")),
        pa.field("received_at", pa.timestamp("us", tz="UTC")),
        pa.field("is_buyer_maker", pa.bool_()),
    ]
)


def _init_duckdb(path: str) -> duckdb.DuckDBPyConnection:
    """Create the bronze schema and raw_trades table if they don't exist."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.raw_trades (
            trade_id        BIGINT NOT NULL,
            symbol          VARCHAR NOT NULL,
            price           DOUBLE NOT NULL,
            quantity        DOUBLE NOT NULL,
            trade_time      TIMESTAMPTZ NOT NULL,
            received_at     TIMESTAMPTZ NOT NULL,
            is_buyer_maker  BOOLEAN NOT NULL,
            -- Binance aggregate trade IDs are only unique within a symbol
            PRIMARY KEY (symbol, trade_id)
        )
        """
    )
    log.info("duckdb_initialized", path=path)
    return conn


def _make_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": CONSUMER_GROUP,
            # Start from earliest unprocessed offset (resumes after restarts)
            "auto.offset.reset": "earliest",
            # Disable auto-commit — offsets are committed after each Parquet flush
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
        }
    )


def _parse_trade(raw: bytes) -> dict | None:
    """
    Decode one Kafka message into a trade, or return None if it is not one.

    The topic is checked against the same TradeEvent model the producer uses.
    A message that is not valid JSON, lacks a field or has a value of the wrong
    type is logged and skipped: raising here would stop the consumer before the
    offset is committed, and it would read the same message again on restart.
    """
    try:
        return TradeEvent.model_validate(json.loads(raw.decode())).to_kafka_value()
    except (json.JSONDecodeError, UnicodeDecodeError, ValidationError) as exc:
        log.warning("message_skipped", error=str(exc))
        return None


def _insert_trade(conn: duckdb.DuckDBPyConnection, trade: dict) -> None:
    # INSERT OR IGNORE keeps the DuckDB table free of redelivered trades
    conn.execute(
        """
        INSERT OR IGNORE INTO raw.raw_trades
            (trade_id, symbol, price, quantity, trade_time, received_at, is_buyer_maker)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            trade["trade_id"],
            trade["symbol"],
            trade["price"],
            trade["quantity"],
            trade["trade_time"],
            trade["received_at"],
            trade["is_buyer_maker"],
        ],
    )


def _flush_parquet(rows: list[dict]) -> Path | None:
    """Write accumulated rows to a new Parquet file and return its path."""
    if not rows:
        return None
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    symbols = "_".join(sorted({row["symbol"] for row in rows}))
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    # Two flushes can fall in the same second, so the timestamp alone is not a unique name.
    path = BRONZE_DIR / f"trades_{symbols}_{ts}_{uuid.uuid4().hex[:8]}.parquet"
    # Kafka values carry timestamps as ISO-8601 strings; Parquet stores them as timestamps
    records = [
        {
            **row,
            "trade_time": datetime.fromisoformat(row["trade_time"]),
            "received_at": datetime.fromisoformat(row["received_at"]),
        }
        for row in rows
    ]
    table = pa.Table.from_pylist(records, schema=BRONZE_SCHEMA)
    # The Silver processor reads every *.parquet file here and may run at any
    # moment. The file is written under another name and renamed when complete,
    # so Silver never sees part of one.
    partial = path.with_suffix(".parquet.tmp")
    pq.write_table(table, str(partial), compression="snappy")
    os.replace(partial, path)
    log.info("parquet_flushed", path=str(path), rows=len(rows))
    return path


def _flush_and_commit(buffer: list[dict], consumer: Consumer) -> None:
    """
    Write the buffered trades to Parquet, then commit the offsets that cover them.

    The order is what makes a crash safe: until the file exists, the offsets
    stay uncommitted and Kafka will deliver those trades again.
    """
    if not buffer:
        return
    _flush_parquet(buffer)
    buffer.clear()
    consumer.commit(asynchronous=False)


def run_consumer() -> None:
    conn = _init_duckdb(DUCKDB_PATH)
    consumer = _make_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    log.info("consumer_started", topic=KAFKA_TOPIC, group=CONSUMER_GROUP)

    buffer: list[dict] = []
    total_written = 0
    running = True
    last_flush_time = time.monotonic()
    # Flush every 30 seconds regardless of row count (handles low-volume feeds)
    flush_interval_seconds = int(os.getenv("PARQUET_FLUSH_INTERVAL_SECONDS", "30"))

    def _shutdown(*_):
        nonlocal running
        log.info("shutdown_requested")
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            # Time-based flush — triggers even when no messages arrive
            if buffer and (time.monotonic() - last_flush_time) >= flush_interval_seconds:
                _flush_and_commit(buffer, consumer)
                last_flush_time = time.monotonic()

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            trade = _parse_trade(msg.value())
            if trade is None:
                continue

            _insert_trade(conn, trade)
            buffer.append(trade)
            total_written += 1

            # Row-count flush
            if len(buffer) >= PARQUET_FLUSH_ROWS:
                _flush_and_commit(buffer, consumer)
                last_flush_time = time.monotonic()

            if total_written % 1000 == 0:
                log.info("consumer_progress", total_written=total_written)

    finally:
        # Flush any remaining buffer rows before shutdown
        _flush_and_commit(buffer, consumer)
        consumer.close()
        conn.close()
        log.info("consumer_stopped", total_written=total_written)


if __name__ == "__main__":
    run_consumer()
