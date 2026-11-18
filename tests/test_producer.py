"""Unit tests for ingest/schemas.py — Pydantic TradeEvent validation."""

import pytest
from pydantic import ValidationError

from ingest.schemas import TradeEvent


VALID_RAW = {
    "e": "aggTrade",
    "E": 1720735200000,
    "s": "BTCUSDT",
    "a": 99999999,
    "p": "67543.21",
    "q": "0.00312",
    "T": 1720735199850,
    "m": False,
}


def test_from_binance_parses_correctly():
    event = TradeEvent.from_binance(VALID_RAW)
    assert event.trade_id == 99999999
    assert event.symbol == "BTCUSDT"
    assert event.price == pytest.approx(67543.21)
    assert event.quantity == pytest.approx(0.00312)
    assert event.is_buyer_maker is False
    assert event.trade_time.tzinfo is not None  # timezone-aware


def test_symbol_coerced_to_uppercase():
    raw = VALID_RAW.copy()
    raw["s"] = "btcusdt"
    event = TradeEvent.from_binance(raw)
    assert event.symbol == "BTCUSDT"


def test_is_buyer_maker_sell_side():
    raw = VALID_RAW.copy()
    raw["m"] = True  # buyer is maker → sell-initiated
    event = TradeEvent.from_binance(raw)
    assert event.is_buyer_maker is True


def test_to_kafka_value_is_json_serialisable():
    import json
    event = TradeEvent.from_binance(VALID_RAW)
    payload = event.to_kafka_value()
    # Should not raise
    json.dumps(payload)
    assert payload["symbol"] == "BTCUSDT"
    assert isinstance(payload["price"], float)


def test_invalid_price_string_raises():
    raw = VALID_RAW.copy()
    raw["p"] = "not_a_number"
    with pytest.raises(ValidationError) as excinfo:
        TradeEvent.from_binance(raw)
    assert [err["loc"] for err in excinfo.value.errors()] == [("price",)]


def test_missing_field_raises():
    raw = {k: v for k, v in VALID_RAW.items() if k != "a"}  # drop trade_id
    with pytest.raises(ValidationError) as excinfo:
        TradeEvent.from_binance(raw)
    assert [err["loc"] for err in excinfo.value.errors()] == [("trade_id",)]
