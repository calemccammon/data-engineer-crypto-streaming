"""
Binance WebSocket → Kafka producer.

Connects to Binance's public aggTrade stream for each configured symbol,
validates each event with Pydantic, and publishes to the `raw-trades` Kafka topic.

Usage:
    uv run python -m ingest.binance_producer
"""

import asyncio
import json
import logging
import os
import signal
import sys

import structlog
import websockets
from confluent_kafka import Producer
from dotenv import load_dotenv
from tenacity import retry, stop_never, wait_exponential, before_sleep_log

from ingest.schemas import TradeEvent

load_dotenv()

log = structlog.get_logger()
_std_log = logging.getLogger(__name__)  # standard logger for tenacity

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW_TRADES", "raw-trades")
SYMBOLS = [s.strip().lower() for s in os.getenv("SYMBOLS", "btcusdt,ethusdt,solusdt").split(",")]

# Binance.US stream URL (same format as Binance.com, accessible from US)
BINANCE_WS_URL = (
    "wss://stream.binance.us:9443/stream?streams="
    + "/".join(f"{s}@aggTrade" for s in SYMBOLS)
)


def _delivery_report(err, msg):
    """Kafka producer delivery callback."""
    if err:
        log.error("kafka_delivery_failed", error=str(err), topic=msg.topic())


def make_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": "binance-producer",
            # Reliability: wait for leader ack; retry up to 5 times
            "acks": "1",
            "retries": 5,
            "retry.backoff.ms": 500,
            # Throughput tuning
            "linger.ms": 50,
            "batch.size": 65536,
        }
    )


@retry(
    stop=stop_never,
    wait=wait_exponential(multiplier=1, min=1, max=30),
    before_sleep=before_sleep_log(_std_log, logging.WARNING),
    reraise=False,
)
async def stream_to_kafka(producer: Producer) -> None:
    """
    Open a single combined WebSocket connection to Binance and forward every
    aggTrade event to Kafka. Retries with exponential back-off on disconnect.
    """
    log.info("connecting_to_binance", url=BINANCE_WS_URL, symbols=SYMBOLS)

    async with websockets.connect(
        BINANCE_WS_URL,
        ping_interval=20,
        ping_timeout=10,
        close_timeout=5,
    ) as ws:
        log.info("connected", symbols=SYMBOLS)

        async for raw_message in ws:
            try:
                outer = json.loads(raw_message)
                # Combined stream wraps each event: {"stream": "btcusdt@aggTrade", "data": {...}}
                data = outer.get("data", outer)

                if data.get("e") != "aggTrade":
                    continue

                event = TradeEvent.from_binance(data)
                value = json.dumps(event.to_kafka_value()).encode()

                # Partition by symbol so each symbol's events stay ordered
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=event.symbol.encode(),
                    value=value,
                    callback=_delivery_report,
                )
                # Poll to trigger delivery callbacks without blocking
                producer.poll(0)

            except Exception as exc:
                log.warning("event_parse_error", error=str(exc))


async def main() -> None:
    producer = make_producer()
    log.info("producer_started", bootstrap=KAFKA_BOOTSTRAP, topic=KAFKA_TOPIC)

    def _shutdown(*_):
        log.info("shutdown_requested")
        producer.flush(timeout=5)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    await stream_to_kafka(producer)


if __name__ == "__main__":
    asyncio.run(main())
