"""Unit tests for consume/silver_processor.py — candle aggregation logic."""

import os
import tempfile
from datetime import datetime, timezone, timedelta

import duckdb
import pytest

# Point at a temp DuckDB for tests
os.environ["DUCKDB_PATH"] = ""  # overridden per test

from consume.silver_processor import _init_silver, _process_batch, _get_watermark


def _make_test_db() -> tuple[str, duckdb.DuckDBPyConnection]:
    """Create an in-memory DuckDB with bronze schema and test trades."""
    tmp = tempfile.mktemp(suffix=".duckdb")
    conn = duckdb.connect(tmp)

    # Bronze schema
    conn.execute("CREATE SCHEMA bronze")
    conn.execute(
        """
        CREATE TABLE bronze.raw_trades (
            trade_id        BIGINT PRIMARY KEY,
            symbol          VARCHAR NOT NULL,
            price           DOUBLE NOT NULL,
            quantity        DOUBLE NOT NULL,
            trade_time      TIMESTAMPTZ NOT NULL,
            received_at     TIMESTAMPTZ NOT NULL,
            is_buyer_maker  BOOLEAN NOT NULL
        )
        """
    )
    _init_silver(conn)
    return tmp, conn


def _insert_trade(conn, trade_id, symbol, price, qty, ts, is_buyer_maker=False):
    now = datetime.now(tz=timezone.utc).isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO bronze.raw_trades VALUES (?, ?, ?, ?, ?, ?, ?)",
        [trade_id, symbol, price, qty, ts.isoformat(), now, is_buyer_maker],
    )


BASE_TIME = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


def test_single_candle_aggregation():
    _, conn = _make_test_db()

    # 3 trades in the same 1-minute window
    _insert_trade(conn, 1, "BTCUSDT", 67000.0, 0.1, BASE_TIME + timedelta(seconds=0))
    _insert_trade(conn, 2, "BTCUSDT", 67100.0, 0.2, BASE_TIME + timedelta(seconds=30))
    _insert_trade(conn, 3, "BTCUSDT", 67050.0, 0.15, BASE_TIME + timedelta(seconds=59))

    _process_batch(conn, from_expr="bronze.raw_trades")

    candles = conn.execute("SELECT * FROM silver.ohlcv_1m").fetchall()
    assert len(candles) == 1

    candle = candles[0]
    symbol, candle_open, open_p, high_p, low_p, close_p, volume, buy_vol, sell_vol, trade_count, vwap, _ = candle

    assert symbol == "BTCUSDT"
    assert open_p == pytest.approx(67000.0)
    assert high_p == pytest.approx(67100.0)
    assert low_p == pytest.approx(67000.0)
    assert close_p == pytest.approx(67050.0)
    assert volume == pytest.approx(0.45)
    assert trade_count == 3


def test_buy_sell_split():
    _, conn = _make_test_db()

    _insert_trade(conn, 10, "ETHUSDT", 3500.0, 1.0, BASE_TIME, is_buyer_maker=False)  # buy
    _insert_trade(conn, 11, "ETHUSDT", 3510.0, 2.0, BASE_TIME + timedelta(seconds=10), is_buyer_maker=True)  # sell

    _process_batch(conn, from_expr="bronze.raw_trades")

    row = conn.execute("SELECT buy_volume, sell_volume FROM silver.ohlcv_1m WHERE symbol = 'ETHUSDT'").fetchone()
    buy_vol, sell_vol = row
    assert buy_vol == pytest.approx(1.0)
    assert sell_vol == pytest.approx(2.0)


def test_two_candles_different_minutes():
    _, conn = _make_test_db()

    _insert_trade(conn, 20, "SOLUSDT", 150.0, 5.0, BASE_TIME)                       # minute 0
    _insert_trade(conn, 21, "SOLUSDT", 151.0, 5.0, BASE_TIME + timedelta(minutes=1)) # minute 1

    _process_batch(conn, from_expr="bronze.raw_trades")

    count = conn.execute("SELECT COUNT(*) FROM silver.ohlcv_1m WHERE symbol = 'SOLUSDT'").fetchone()[0]
    assert count == 2


def test_vwap_calculation():
    _, conn = _make_test_db()

    # VWAP = sum(price * qty) / sum(qty)
    # Trade A: price=100, qty=1 → notional=100
    # Trade B: price=200, qty=3 → notional=600
    # VWAP = 700 / 4 = 175
    _insert_trade(conn, 30, "BTCUSDT", 100.0, 1.0, BASE_TIME)
    _insert_trade(conn, 31, "BTCUSDT", 200.0, 3.0, BASE_TIME + timedelta(seconds=5))

    _process_batch(conn, from_expr="bronze.raw_trades")

    vwap = conn.execute("SELECT vwap FROM silver.ohlcv_1m WHERE symbol = 'BTCUSDT'").fetchone()[0]
    assert vwap == pytest.approx(175.0)


def test_idempotent_rerun():
    """Running _process_batch twice should not create duplicate candles."""
    _, conn = _make_test_db()
    _insert_trade(conn, 40, "BTCUSDT", 67000.0, 0.1, BASE_TIME)

    _process_batch(conn, from_expr="bronze.raw_trades")
    _process_batch(conn, from_expr="bronze.raw_trades")

    count = conn.execute("SELECT COUNT(*) FROM silver.ohlcv_1m").fetchone()[0]
    assert count == 1
