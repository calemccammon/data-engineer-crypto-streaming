"""
Kafka → Bronze consumer.

Reads trade events from the `raw-trades` Kafka topic and writes them to:
  1. DuckDB `raw.raw_trades` table in data/bronze.duckdb (append-only, deduped by symbol and trade_id)
  2. Rolling Parquet snapshots in data/bronze/ (every PARQUET_FLUSH_ROWS rows)

Bronze is the landing zone — data is written as-is with minimal transformation.
The only guarantee the DuckDB table makes is uniqueness on (symbol, trade_id).
The Parquet files can repeat a trade when Kafka redelivers it. The Silver
processor collapses those on read.

Usage:
    uv run python -m consume.bronze_consumer
"""

import json
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

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
            # Disable auto-commit — we commit after successful DuckDB write
            "enable.auto.commit": False,
            "session.timeout.ms": 30000,
        }
    )


def _flush_parquet(rows: list[dict], symbol_hint: str) -> None:
    """Write accumulated rows to a timestamped Parquet file."""
    if not rows:
        return
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = BRONZE_DIR / f"trades_{symbol_hint}_{ts}.parquet"
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
    pq.write_table(table, str(path), compression="snappy")
    log.info("parquet_flushed", path=str(path), rows=len(rows))


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
    FLUSH_INTERVAL_SECONDS = int(os.getenv("PARQUET_FLUSH_INTERVAL_SECONDS", "30"))

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
            time_to_flush = (time.monotonic() - last_flush_time) >= FLUSH_INTERVAL_SECONDS
            if buffer and time_to_flush:
                symbols = {r["symbol"] for r in buffer}
                _flush_parquet(buffer, "_".join(sorted(symbols)))
                buffer.clear()
                last_flush_time = time.monotonic()

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                event = json.loads(msg.value().decode())
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                log.warning("message_parse_error", error=str(exc))
                continue

            # INSERT OR IGNORE keeps the DuckDB table free of redelivered trades
            conn.execute(
                """
                INSERT OR IGNORE INTO raw.raw_trades
                    (trade_id, symbol, price, quantity, trade_time, received_at, is_buyer_maker)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    event["trade_id"],
                    event["symbol"],
                    event["price"],
                    event["quantity"],
                    event["trade_time"],
                    event["received_at"],
                    event["is_buyer_maker"],
                ],
            )

            buffer.append(event)
            total_written += 1

            # Commit offset after successful write
            consumer.commit(asynchronous=True)

            # Row-count flush
            if len(buffer) >= PARQUET_FLUSH_ROWS:
                symbols = {r["symbol"] for r in buffer}
                _flush_parquet(buffer, "_".join(sorted(symbols)))
                buffer.clear()
                last_flush_time = time.monotonic()

            if total_written % 1000 == 0:
                log.info("consumer_progress", total_written=total_written)

    finally:
        # Flush any remaining buffer rows before shutdown
        if buffer:
            symbols = {r["symbol"] for r in buffer}
            _flush_parquet(buffer, "_".join(sorted(symbols)))
        consumer.close()
        conn.close()
        log.info("consumer_stopped", total_written=total_written)


if __name__ == "__main__":
    run_consumer()
