"""Unit tests for consume/bronze_consumer.py — Parquet snapshot writing."""

import duckdb

from consume import bronze_consumer


def test_flush_parquet_writes_typed_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze_consumer, "BRONZE_DIR", tmp_path)

    # Rows as they arrive from Kafka: timestamps are ISO-8601 strings
    rows = [
        {
            "trade_id": 1,
            "symbol": "BTCUSDT",
            "price": 67000.5,
            "quantity": 0.25,
            "trade_time": "2024-01-15T10:00:00.123000+00:00",
            "received_at": "2024-01-15T10:00:01+00:00",
            "is_buyer_maker": True,
        },
        {
            "trade_id": 2,
            "symbol": "ETHUSDT",
            "price": 3500.0,
            "quantity": 1.5,
            "trade_time": "2024-01-15T10:00:02+00:00",
            "received_at": "2024-01-15T10:00:03+00:00",
            "is_buyer_maker": False,
        },
    ]

    bronze_consumer._flush_parquet(rows, "BTCUSDT_ETHUSDT")

    files = list(tmp_path.glob("*.parquet"))
    assert len(files) == 1

    conn = duckdb.connect()
    result = conn.execute(
        f"""
        SELECT
            trade_id,
            symbol,
            price,
            quantity,
            is_buyer_maker,
            trade_time = TIMESTAMPTZ '2024-01-15 10:00:00.123+00'
                OR trade_time = TIMESTAMPTZ '2024-01-15 10:00:02+00' AS trade_time_matches,
            typeof(received_at) AS received_at_type
        FROM read_parquet('{files[0].as_posix()}')
        ORDER BY trade_id
        """
    ).fetchall()
    conn.close()

    assert result == [
        (1, "BTCUSDT", 67000.5, 0.25, True, True, "TIMESTAMP WITH TIME ZONE"),
        (2, "ETHUSDT", 3500.0, 1.5, False, True, "TIMESTAMP WITH TIME ZONE"),
    ]


def test_flush_parquet_with_no_rows_writes_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze_consumer, "BRONZE_DIR", tmp_path)

    bronze_consumer._flush_parquet([], "BTCUSDT")

    assert list(tmp_path.glob("*.parquet")) == []
