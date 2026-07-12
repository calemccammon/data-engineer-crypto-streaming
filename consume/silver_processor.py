"""
Bronze → Silver processor (micro-batch).

Reads raw trades from Parquet files in data/bronze/ (written by the Kafka consumer),
groups them into 1-minute OHLCV candles, and writes to silver.ohlcv_1m.

Using Parquet as the Bronze→Silver interface avoids cross-process DuckDB file
locking: the bronze consumer exclusively owns bronze.duckdb for writes, and
the silver processor reads the same data via the Parquet snapshots it deposits
in data/bronze/. DuckDB reads Parquet natively and fast via read_parquet().

Key concepts demonstrated:
- Micro-batch processing (runs every POLL_INTERVAL seconds)
- Event time bucketing (trades bucketed by the minute they *occurred*, not processed)
- Late-arrival handling (re-process the last LATE_WINDOW seconds on every run)
- DuckDB UPSERT (INSERT OR REPLACE for idempotent candle updates)
- Parquet as an inter-process data exchange format

Usage:
    uv run python -m consume.silver_processor

Tables written (in data/crypto.duckdb):
    silver.ohlcv_1m     — 1-minute OHLCV candles per symbol
    silver.trades_clean — deduplicated trades with side and notional_value
"""

import os
import signal
import time
from pathlib import Path

import duckdb
import structlog
from dotenv import load_dotenv

load_dotenv()

log = structlog.get_logger()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/crypto.duckdb")
BRONZE_DIR = Path(os.getenv("DATA_DIR", "data")) / "bronze"
LATE_WINDOW = int(os.getenv("LATE_ARRIVAL_WINDOW_SECONDS", "90"))
POLL_INTERVAL = int(os.getenv("SILVER_POLL_INTERVAL_SECONDS", "30"))


def _init_silver(conn: duckdb.DuckDBPyConnection) -> None:
    """Create Silver schema and tables if they don't exist."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.ohlcv_1m (
            symbol          VARCHAR NOT NULL,
            candle_open     TIMESTAMPTZ NOT NULL,
            open_price      DOUBLE NOT NULL,
            high_price      DOUBLE NOT NULL,
            low_price       DOUBLE NOT NULL,
            close_price     DOUBLE NOT NULL,
            volume          DOUBLE NOT NULL,
            buy_volume      DOUBLE NOT NULL,
            sell_volume     DOUBLE NOT NULL,
            trade_count     BIGINT NOT NULL,
            vwap            DOUBLE NOT NULL,
            updated_at      TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (symbol, candle_open)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.trades_clean (
            trade_id        BIGINT PRIMARY KEY,
            symbol          VARCHAR NOT NULL,
            price           DOUBLE NOT NULL,
            quantity        DOUBLE NOT NULL,
            notional_value  DOUBLE NOT NULL,
            side            VARCHAR NOT NULL,
            trade_time      TIMESTAMPTZ NOT NULL,
            received_at     TIMESTAMPTZ NOT NULL,
            candle_open     TIMESTAMPTZ NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS silver._watermark (
            id              INTEGER PRIMARY KEY DEFAULT 1,
            last_processed  TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00'
        )
        """
    )
    conn.execute(
        "INSERT OR IGNORE INTO silver._watermark (id, last_processed) VALUES (1, '1970-01-01 00:00:00+00')"
    )
    log.info("silver_initialized")


def _get_watermark(conn: duckdb.DuckDBPyConnection) -> object:
    return conn.execute("SELECT last_processed FROM silver._watermark WHERE id = 1").fetchone()[0]


def _set_watermark(conn: duckdb.DuckDBPyConnection, ts: object) -> None:
    conn.execute("UPDATE silver._watermark SET last_processed = ? WHERE id = 1", [ts])


def _get_parquet_glob() -> str | None:
    """Return a glob string for all Parquet files in the bronze dir, or None if none exist."""
    if not BRONZE_DIR.exists():
        return None
    files = list(BRONZE_DIR.glob("*.parquet"))
    if not files:
        return None
    # Use forward slashes for DuckDB glob on Windows
    return str(BRONZE_DIR / "*.parquet").replace("\\", "/")


def _process_batch(conn: duckdb.DuckDBPyConnection, from_expr: str | None = None) -> int:
    """
    Read Bronze data and upsert Silver tables.

    Args:
        from_expr: SQL table/expression to read trades from.  If None (default),
                   uses ``read_parquet('<bronze_dir>/*.parquet')``.  Pass a plain
                   table reference such as ``"bronze.raw_trades"`` in tests so the
                   function queries the caller's in-memory data instead of disk files.

    Returns:
        Number of trades processed (0 if nothing new).
    """
    if from_expr is None:
        glob = _get_parquet_glob()
        if not glob:
            return 0
        source = f"read_parquet('{glob}')"
        max_ts_source = source
    else:
        source = from_expr
        max_ts_source = from_expr

    watermark = _get_watermark(conn)

    # Count how many new trades are in scope
    count_row = conn.execute(
        f"""
        SELECT COUNT(*) FROM {source}
        WHERE trade_time >= (? - INTERVAL (?) SECOND)
        """,
        [watermark, LATE_WINDOW],
    ).fetchone()
    count = count_row[0] if count_row else 0

    if count == 0:
        return 0

    # ----- trades_clean upsert -----
    conn.execute(
        f"""
        INSERT OR REPLACE INTO silver.trades_clean
        SELECT
            trade_id,
            symbol,
            price,
            quantity,
            price * quantity                                        AS notional_value,
            CASE WHEN is_buyer_maker THEN 'sell' ELSE 'buy' END    AS side,
            trade_time,
            received_at,
            date_trunc('minute', trade_time)                       AS candle_open
        FROM {source}
        WHERE trade_time >= (? - INTERVAL (?) SECOND)
        """,
        [watermark, LATE_WINDOW],
    )

    # ----- ohlcv_1m upsert — aggregate to 1-minute candles -----
    conn.execute(
        f"""
        INSERT OR REPLACE INTO silver.ohlcv_1m
        SELECT
            symbol,
            date_trunc('minute', trade_time)                       AS candle_open,
            FIRST(price ORDER BY trade_time)                       AS open_price,
            MAX(price)                                             AS high_price,
            MIN(price)                                             AS low_price,
            LAST(price ORDER BY trade_time)                        AS close_price,
            SUM(quantity)                                          AS volume,
            SUM(CASE WHEN NOT is_buyer_maker THEN quantity ELSE 0 END) AS buy_volume,
            SUM(CASE WHEN is_buyer_maker THEN quantity ELSE 0 END)     AS sell_volume,
            COUNT(*)                                               AS trade_count,
            SUM(price * quantity) / NULLIF(SUM(quantity), 0)      AS vwap,
            NOW()                                                  AS updated_at
        FROM {source}
        WHERE trade_time >= (? - INTERVAL (?) SECOND)
        GROUP BY symbol, date_trunc('minute', trade_time)
        """,
        [watermark, LATE_WINDOW],
    )

    # Advance watermark
    max_ts = conn.execute(
        f"SELECT MAX(trade_time) FROM {max_ts_source}"
    ).fetchone()[0]
    if max_ts:
        _set_watermark(conn, max_ts)

    return count


def run_processor(once: bool = False) -> None:
    """
    Run the Silver micro-batch processor.

    Opens crypto.duckdb only during each batch write, then closes it immediately.
    This lets the dashboard (and dbt) connect in between runs.

    Args:
        once: If True, run a single batch and return (used by Dagster assets).
    """
    # Initialize tables on first run
    conn = duckdb.connect(DUCKDB_PATH)
    _init_silver(conn)
    conn.close()

    running = True

    def _shutdown(*_):
        nonlocal running
        log.info("shutdown_requested")
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("silver_processor_started", poll_interval=POLL_INTERVAL, late_window=LATE_WINDOW)

    while running:
        start = time.monotonic()
        conn = duckdb.connect(DUCKDB_PATH)
        try:
            count = _process_batch(conn)
        finally:
            conn.close()
        elapsed = time.monotonic() - start
        if count:
            log.info("batch_processed", trades=count, elapsed_ms=round(elapsed * 1000))
        else:
            log.debug("no_new_trades")
        if once:
            break
        time.sleep(POLL_INTERVAL)

    log.info("silver_processor_stopped")


if __name__ == "__main__":
    run_processor()
