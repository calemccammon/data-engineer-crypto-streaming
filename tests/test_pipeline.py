"""
The pipeline from Bronze Parquet files to the Gold tables, as Dagster runs it.

Nothing exists at the start but the files the Bronze consumer writes: no
database, and no views or tables that an earlier dbt run might have left.
"""

from datetime import datetime, timedelta, timezone

import duckdb
import pytest
from dagster import materialize

from consume import bronze_consumer, silver_processor
from orchestrate import assets
from orchestrate.definitions import defs

BASE_TIME = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


def _trade(trade_id: int, symbol: str, price: float, at: datetime) -> dict:
    return {
        "trade_id": trade_id,
        "symbol": symbol,
        "price": price,
        "quantity": 0.5,
        "trade_time": at.isoformat(),
        "received_at": (at + timedelta(seconds=1)).isoformat(),
        "is_buyer_maker": trade_id % 2 == 0,
    }


@pytest.fixture
def fresh_data_dir(tmp_path, monkeypatch):
    """Point every stage at an empty directory."""
    database = str(tmp_path / "crypto.duckdb")
    monkeypatch.setattr(bronze_consumer, "BRONZE_DIR", tmp_path / "bronze")
    monkeypatch.setattr(silver_processor, "BRONZE_DIR", tmp_path / "bronze")
    monkeypatch.setattr(silver_processor, "DUCKDB_PATH", database)
    monkeypatch.setattr(assets, "DUCKDB_PATH", database)
    return database


def test_definitions_load():
    assert defs.resolve_job_def("silver_to_gold_job") is not None
    assert {key.to_user_string() for key in defs.resolve_asset_graph().get_all_asset_keys()} == {
        "bronze_raw_trades",
        "silver_candles",
        "gold_models",
    }


def test_silver_and_gold_build_from_bronze_files_on_an_empty_database(fresh_data_dir):
    # Thirty minutes of trades for two symbols, three a minute, in two files
    trades = [
        _trade(minute * 3 + n, symbol, price + minute + n, BASE_TIME + timedelta(minutes=minute, seconds=n * 20))
        for symbol, price in (("BTCUSDT", 67000.0), ("ETHUSDT", 3500.0))
        for minute in range(30)
        for n in range(3)
    ]
    bronze_consumer._flush_parquet(trades[:100])
    # The second file repeats ten trades, as it would after a redelivery
    bronze_consumer._flush_parquet(trades[90:])

    result = materialize([assets.bronze_raw_trades, assets.silver_candles, assets.gold_models])

    assert result.success
    conn = duckdb.connect(fresh_data_dir, read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM silver.trades_clean").fetchone()[0] == 180
        assert conn.execute("SELECT COUNT(*) FROM silver.ohlcv_1m").fetchone()[0] == 60
        assert conn.execute("SELECT COUNT(*) FROM gold.gold_vwap").fetchone()[0] == 60
        assert conn.execute("SELECT symbol FROM gold.gold_symbol_summary ORDER BY symbol").fetchall() == [
            ("BTCUSDT",),
            ("ETHUSDT",),
        ]
    finally:
        conn.close()

    # A second run is incremental and must leave the same rows behind
    assert materialize([assets.bronze_raw_trades, assets.silver_candles, assets.gold_models]).success
    conn = duckdb.connect(fresh_data_dir, read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM gold.gold_vwap").fetchone()[0] == 60
    finally:
        conn.close()
