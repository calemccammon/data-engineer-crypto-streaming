"""Pydantic models for Binance trade stream events."""

from datetime import datetime, timezone
from pydantic import BaseModel, field_validator


class TradeEvent(BaseModel):
    """
    Represents a single trade from Binance's aggregate trade stream.

    Binance raw message shape (aggTrade stream):
    {
        "e": "aggTrade",   # Event type
        "E": 123456789,    # Event time (ms)
        "s": "BTCUSDT",    # Symbol
        "a": 5933014,      # Aggregate trade ID
        "p": "0.001",      # Price
        "q": "100",        # Quantity
        "T": 123456785,    # Trade time (ms)
        "m": true          # Is the buyer the market maker?
    }
    """

    trade_id: int
    symbol: str
    price: float
    quantity: float
    trade_time: datetime
    received_at: datetime
    is_buyer_maker: bool  # True = sell-initiated (maker is buyer), False = buy-initiated

    @field_validator("symbol")
    @classmethod
    def symbol_upper(cls, v: str) -> str:
        return v.upper()

    @classmethod
    def from_binance(cls, raw: dict) -> "TradeEvent":
        """Parse a raw Binance aggTrade message into a TradeEvent."""
        return cls(
            trade_id=raw["a"],
            symbol=raw["s"],
            price=float(raw["p"]),
            quantity=float(raw["q"]),
            trade_time=datetime.fromtimestamp(raw["T"] / 1000, tz=timezone.utc),
            received_at=datetime.now(tz=timezone.utc),
            is_buyer_maker=raw["m"],
        )

    def to_kafka_value(self) -> dict:
        """Serialise to a JSON-safe dict for Kafka."""
        return {
            "trade_id": self.trade_id,
            "symbol": self.symbol,
            "price": self.price,
            "quantity": self.quantity,
            "trade_time": self.trade_time.isoformat(),
            "received_at": self.received_at.isoformat(),
            "is_buyer_maker": self.is_buyer_maker,
        }
