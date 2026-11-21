"""Unit tests for consume/bronze_consumer.py — message checking, Parquet writing and offset commits."""

import json

import duckdb
import pytest

from consume import bronze_consumer

TRADE = {
    "trade_id": 1,
    "symbol": "BTCUSDT",
    "price": 67000.5,
    "quantity": 0.25,
    "trade_time": "2024-01-15T10:00:00.123000+00:00",
    "received_at": "2024-01-15T10:00:01+00:00",
    "is_buyer_maker": True,
}


@pytest.fixture
def bronze_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze_consumer, "BRONZE_DIR", tmp_path)
    return tmp_path


class FakeConsumer:
    """Stands in for the Kafka consumer and records what was on disk at each commit."""

    def __init__(self, bronze_dir):
        self._bronze_dir = bronze_dir
        self.files_at_commit: list[list[str]] = []

    def commit(self, asynchronous=True):
        assert asynchronous is False, "an offset is only safe once the broker has confirmed it"
        self.files_at_commit.append(sorted(p.name for p in self._bronze_dir.glob("*.parquet")))


# --- _parse_trade ---


def test_parse_trade_returns_the_trade():
    assert bronze_consumer._parse_trade(json.dumps(TRADE).encode()) == TRADE


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"\xff\xfe",
        json.dumps({k: v for k, v in TRADE.items() if k != "price"}).encode(),
        json.dumps({**TRADE, "quantity": None}).encode(),
        json.dumps({**TRADE, "trade_time": "yesterday"}).encode(),
        json.dumps([TRADE]).encode(),
    ],
    ids=["not-json", "not-utf8", "missing-field", "null-field", "bad-timestamp", "not-an-object"],
)
def test_parse_trade_skips_a_message_that_is_not_a_trade(raw):
    # Raising would stop the consumer with the offset uncommitted, and it would
    # read the same message again every time it was restarted.
    assert bronze_consumer._parse_trade(raw) is None


def test_insert_trade_ignores_a_redelivered_trade(tmp_path):
    conn = bronze_consumer._init_duckdb(str(tmp_path / "bronze.duckdb"))

    bronze_consumer._insert_trade(conn, TRADE)
    bronze_consumer._insert_trade(conn, TRADE)
    # The same ID on another symbol is another trade
    bronze_consumer._insert_trade(conn, {**TRADE, "symbol": "ETHUSDT"})

    rows = conn.execute("SELECT symbol, trade_id FROM raw.raw_trades ORDER BY symbol").fetchall()
    conn.close()
    assert rows == [("BTCUSDT", 1), ("ETHUSDT", 1)]


# --- _flush_parquet ---


def test_flush_parquet_writes_typed_rows(bronze_dir):
    # Rows as they arrive from Kafka: timestamps are ISO-8601 strings
    rows = [
        TRADE,
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

    path = bronze_consumer._flush_parquet(rows)

    assert list(bronze_dir.glob("*.parquet")) == [path]
    assert path.name.startswith("trades_BTCUSDT_ETHUSDT_")

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
        FROM read_parquet('{path.as_posix()}')
        ORDER BY trade_id
        """
    ).fetchall()
    conn.close()

    assert result == [
        (1, "BTCUSDT", 67000.5, 0.25, True, True, "TIMESTAMP WITH TIME ZONE"),
        (2, "ETHUSDT", 3500.0, 1.5, False, True, "TIMESTAMP WITH TIME ZONE"),
    ]


def test_flush_parquet_with_no_rows_writes_nothing(bronze_dir):
    assert bronze_consumer._flush_parquet([]) is None

    assert list(bronze_dir.iterdir()) == []


def test_flushes_in_the_same_second_do_not_overwrite_each_other(bronze_dir):
    first = bronze_consumer._flush_parquet([TRADE])
    second = bronze_consumer._flush_parquet([{**TRADE, "trade_id": 2}])

    assert first != second
    assert sorted(bronze_dir.glob("*.parquet")) == sorted([first, second])


def test_a_file_is_not_visible_to_silver_until_it_is_complete(bronze_dir, monkeypatch):
    seen_while_writing = []
    write_table = bronze_consumer.pq.write_table

    def write_and_look(table, where, **kwargs):
        write_table(table, where, **kwargs)
        # What the Silver processor's glob would match at this moment
        seen_while_writing.extend(bronze_dir.glob("*.parquet"))

    monkeypatch.setattr(bronze_consumer.pq, "write_table", write_and_look)

    path = bronze_consumer._flush_parquet([TRADE])

    assert seen_while_writing == []
    assert list(bronze_dir.iterdir()) == [path]


# --- _flush_and_commit ---


def test_offsets_are_committed_only_after_the_trades_are_on_disk(bronze_dir):
    consumer = FakeConsumer(bronze_dir)
    buffer = [TRADE]

    bronze_consumer._flush_and_commit(buffer, consumer)

    assert buffer == []
    assert len(consumer.files_at_commit) == 1
    assert len(consumer.files_at_commit[0]) == 1


def test_offsets_are_not_committed_when_the_write_fails(bronze_dir, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(bronze_consumer.pq, "write_table", fail)
    consumer = FakeConsumer(bronze_dir)
    buffer = [TRADE]

    with pytest.raises(OSError):
        bronze_consumer._flush_and_commit(buffer, consumer)

    # Uncommitted, so Kafka delivers the trade again after a restart
    assert consumer.files_at_commit == []
    assert buffer == [TRADE]


def test_nothing_is_committed_for_an_empty_buffer(bronze_dir):
    consumer = FakeConsumer(bronze_dir)

    bronze_consumer._flush_and_commit([], consumer)

    assert consumer.files_at_commit == []
