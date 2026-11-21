"""Unit tests for consume/silver_processor.py — candle aggregation logic."""

from datetime import datetime, timezone, timedelta

import duckdb
import pytest

from consume.silver_processor import _init_silver, _process_batch


@pytest.fixture
def conn(tmp_path):
    """A DuckDB file in pytest's temp dir with an empty bronze table and the silver tables."""
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))

    # Bronze schema
    conn.execute("CREATE SCHEMA bronze")
    conn.execute(
        """
        CREATE TABLE bronze.raw_trades (
            trade_id        BIGINT NOT NULL,
            symbol          VARCHAR NOT NULL,
            price           DOUBLE NOT NULL,
            quantity        DOUBLE NOT NULL,
            trade_time      TIMESTAMPTZ NOT NULL,
            received_at     TIMESTAMPTZ NOT NULL,
            is_buyer_maker  BOOLEAN NOT NULL,
            PRIMARY KEY (symbol, trade_id)
        )
        """
    )
    _init_silver(conn)
    yield conn
    conn.close()


def _insert_trade(conn, trade_id, symbol, price, qty, ts, is_buyer_maker=False):
    now = datetime.now(tz=timezone.utc).isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO bronze.raw_trades VALUES (?, ?, ?, ?, ?, ?, ?)",
        [trade_id, symbol, price, qty, ts.isoformat(), now, is_buyer_maker],
    )


BASE_TIME = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


def test_single_candle_aggregation(conn):
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


def test_buy_sell_split(conn):
    _insert_trade(conn, 10, "ETHUSDT", 3500.0, 1.0, BASE_TIME, is_buyer_maker=False)  # buy
    _insert_trade(conn, 11, "ETHUSDT", 3510.0, 2.0, BASE_TIME + timedelta(seconds=10), is_buyer_maker=True)  # sell

    _process_batch(conn, from_expr="bronze.raw_trades")

    row = conn.execute("SELECT buy_volume, sell_volume FROM silver.ohlcv_1m WHERE symbol = 'ETHUSDT'").fetchone()
    buy_vol, sell_vol = row
    assert buy_vol == pytest.approx(1.0)
    assert sell_vol == pytest.approx(2.0)


def test_two_candles_different_minutes(conn):
    _insert_trade(conn, 20, "SOLUSDT", 150.0, 5.0, BASE_TIME)                       # minute 0
    _insert_trade(conn, 21, "SOLUSDT", 151.0, 5.0, BASE_TIME + timedelta(minutes=1)) # minute 1

    _process_batch(conn, from_expr="bronze.raw_trades")

    count = conn.execute("SELECT COUNT(*) FROM silver.ohlcv_1m WHERE symbol = 'SOLUSDT'").fetchone()[0]
    assert count == 2


def test_vwap_calculation(conn):
    # VWAP = sum(price * qty) / sum(qty)
    # Trade A: price=100, qty=1 → notional=100
    # Trade B: price=200, qty=3 → notional=600
    # VWAP = 700 / 4 = 175
    _insert_trade(conn, 30, "BTCUSDT", 100.0, 1.0, BASE_TIME)
    _insert_trade(conn, 31, "BTCUSDT", 200.0, 3.0, BASE_TIME + timedelta(seconds=5))

    _process_batch(conn, from_expr="bronze.raw_trades")

    vwap = conn.execute("SELECT vwap FROM silver.ohlcv_1m WHERE symbol = 'BTCUSDT'").fetchone()[0]
    assert vwap == pytest.approx(175.0)


def test_idempotent_rerun(conn):
    """Running _process_batch twice should not create duplicate candles."""
    _insert_trade(conn, 40, "BTCUSDT", 67000.0, 0.1, BASE_TIME)

    _process_batch(conn, from_expr="bronze.raw_trades")
    _process_batch(conn, from_expr="bronze.raw_trades")

    count = conn.execute("SELECT COUNT(*) FROM silver.ohlcv_1m").fetchone()[0]
    assert count == 1


def test_late_window_rebuilds_whole_candle(conn):
    """
    The late-arrival cutoff falls inside the 10:00 candle on the second batch.
    The candle must still be built from every trade in that minute.
    """
    _insert_trade(conn, 50, "BTCUSDT", 67000.0, 0.1, BASE_TIME + timedelta(seconds=5))
    _insert_trade(conn, 51, "BTCUSDT", 67100.0, 0.2, BASE_TIME + timedelta(seconds=50))
    _insert_trade(conn, 52, "BTCUSDT", 67200.0, 0.3, BASE_TIME + timedelta(seconds=100))

    # First batch processes everything and leaves the watermark at 10:01:40.
    _process_batch(conn, from_expr="bronze.raw_trades")

    # Second batch looks back 90 seconds, to 10:00:10. Trade 50 (10:00:05) is
    # before that point but belongs to a candle the batch rewrites.
    _process_batch(conn, from_expr="bronze.raw_trades")

    open_p, volume, trade_count = conn.execute(
        """
        SELECT open_price, volume, trade_count
        FROM silver.ohlcv_1m
        WHERE symbol = 'BTCUSDT'
        ORDER BY candle_open
        LIMIT 1
        """
    ).fetchone()
    assert open_p == pytest.approx(67000.0)
    assert volume == pytest.approx(0.3)
    assert trade_count == 2


def test_same_trade_id_on_two_symbols_keeps_both_trades(conn):
    """Binance aggregate trade IDs are per symbol, so two symbols can share one."""
    _insert_trade(conn, 60, "BTCUSDT", 67000.0, 0.1, BASE_TIME)
    _insert_trade(conn, 60, "ETHUSDT", 3500.0, 1.0, BASE_TIME)

    _process_batch(conn, from_expr="bronze.raw_trades")

    rows = conn.execute(
        "SELECT symbol, price FROM silver.trades_clean WHERE trade_id = 60 ORDER BY symbol"
    ).fetchall()
    assert rows == [("BTCUSDT", 67000.0), ("ETHUSDT", 3500.0)]


def test_repeated_trade_is_counted_once(conn):
    """A trade that appears twice in the Bronze files must not double the candle."""
    _insert_trade(conn, 70, "BTCUSDT", 67000.0, 0.1, BASE_TIME)
    _insert_trade(conn, 71, "BTCUSDT", 67100.0, 0.2, BASE_TIME + timedelta(seconds=20))
    conn.execute(
        """
        CREATE TABLE bronze.redelivered AS
        SELECT * FROM bronze.raw_trades
        UNION ALL
        SELECT * FROM bronze.raw_trades
        """
    )

    _process_batch(conn, from_expr="bronze.redelivered")

    volume, trade_count = conn.execute(
        "SELECT volume, trade_count FROM silver.ohlcv_1m WHERE symbol = 'BTCUSDT'"
    ).fetchone()
    assert volume == pytest.approx(0.3)
    assert trade_count == 2
    assert conn.execute("SELECT COUNT(*) FROM silver.trades_clean").fetchone()[0] == 2


def test_a_symbol_that_is_behind_the_others_is_not_passed_over(conn):
    """
    After a consumer restart the symbols are read back unevenly, so one batch can
    see trades far newer for one symbol than the next batch brings for another.
    """
    _insert_trade(conn, 80, "BTCUSDT", 67000.0, 0.1, BASE_TIME + timedelta(minutes=10))
    _process_batch(conn, from_expr="bronze.raw_trades")

    # ETH's backlog arrives next: ten minutes older than the newest BTC trade,
    # which is well outside the 90-second late window.
    _insert_trade(conn, 81, "ETHUSDT", 3500.0, 1.0, BASE_TIME)
    _insert_trade(conn, 82, "ETHUSDT", 3510.0, 2.0, BASE_TIME + timedelta(seconds=30))
    _process_batch(conn, from_expr="bronze.raw_trades")

    candle = conn.execute(
        "SELECT open_price, close_price, trade_count FROM silver.ohlcv_1m WHERE symbol = 'ETHUSDT'"
    ).fetchall()
    assert candle == [(3500.0, 3510.0, 2)]
    assert conn.execute("SELECT COUNT(*) FROM silver.trades_clean WHERE symbol = 'ETHUSDT'").fetchone()[0] == 2


def test_a_trade_later_than_its_own_symbols_window_is_still_left_out(conn):
    """The per-symbol watermark does not widen the late window within a symbol."""
    _insert_trade(conn, 90, "BTCUSDT", 67000.0, 0.1, BASE_TIME + timedelta(minutes=10))
    _process_batch(conn, from_expr="bronze.raw_trades")

    _insert_trade(conn, 91, "BTCUSDT", 66000.0, 0.1, BASE_TIME)
    _process_batch(conn, from_expr="bronze.raw_trades")

    assert conn.execute("SELECT COUNT(*) FROM silver.ohlcv_1m").fetchone()[0] == 1


def test_trades_at_the_same_instant_open_and_close_in_trade_id_order(conn):
    """One order filled at several prices gives trades with the same millisecond."""
    # Inserted out of order, so the answer cannot come from the order of the rows
    _insert_trade(conn, 102, "BTCUSDT", 67020.0, 0.1, BASE_TIME)
    _insert_trade(conn, 100, "BTCUSDT", 67000.0, 0.1, BASE_TIME)
    _insert_trade(conn, 101, "BTCUSDT", 67010.0, 0.1, BASE_TIME)

    _process_batch(conn, from_expr="bronze.raw_trades")

    assert conn.execute("SELECT open_price, close_price FROM silver.ohlcv_1m").fetchall() == [
        (67000.0, 67020.0)
    ]


def test_a_batch_that_fails_changes_nothing(conn):
    """The trades, the candles and the watermarks are written together."""
    _insert_trade(conn, 110, "BTCUSDT", 67000.0, 0.1, BASE_TIME)
    # A candle cannot be stored without a VWAP, and zero volume has none
    _insert_trade(conn, 111, "ETHUSDT", 3500.0, 0.0, BASE_TIME)

    with pytest.raises(duckdb.ConstraintException):
        _process_batch(conn, from_expr="bronze.raw_trades")

    for table in ("trades_clean", "ohlcv_1m", "_symbol_watermark"):
        assert conn.execute(f"SELECT COUNT(*) FROM silver.{table}").fetchone()[0] == 0, table
