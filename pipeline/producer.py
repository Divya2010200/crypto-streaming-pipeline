"""Stream live public trades from Binance into Kafka."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time

from confluent_kafka import Producer
from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from pipeline.events import InvalidTrade, normalize_trade, reconnect_delay, stream_path

RAW_TOPIC = "crypto.trades.raw"
DEAD_LETTER_TOPIC = "crypto.trades.dead-letter"
ENDPOINTS = [
    "wss://stream.binance.com:9443",
    "wss://stream.binance.com:443",
    "wss://data-stream.binance.vision",
]
LOG_EVERY_SECONDS = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("producer")


class Stats:
    received = 0
    rejected = 0
    delivered = 0
    failed = 0


def on_delivery(error, _message) -> None:
    if error:
        Stats.failed += 1
        logger.error("Kafka delivery failed: %s", error)
    else:
        Stats.delivered += 1


def send(producer: Producer, topic: str, key: str | None, value: str | bytes) -> None:
    while True:
        try:
            producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
            producer.poll(0)
            return
        except BufferError:
            producer.poll(0.2)


def handle_message(producer: Producer, message: str | bytes) -> None:
    Stats.received += 1
    try:
        trade = normalize_trade(message, int(time.time() * 1000))
    except InvalidTrade as error:
        Stats.rejected += 1
        logger.warning("Rejected message: %s", error)
        send(producer, DEAD_LETTER_TOPIC, None, message)
        return
    send(producer, RAW_TOPIC, trade["symbol"], json.dumps(trade, separators=(",", ":")))


async def stream(producer: Producer, symbols: list[str], stop: asyncio.Event) -> None:
    attempt = 0
    path = stream_path(symbols)
    last_log = time.monotonic()
    while not stop.is_set():
        endpoint = ENDPOINTS[attempt % len(ENDPOINTS)]
        try:
            logger.info("Connecting to %s", endpoint)
            async with connect(endpoint + path, open_timeout=15, max_queue=2048) as socket:
                logger.info("Connected for %s", ",".join(symbols))
                attempt = 0
                while not stop.is_set():
                    try:
                        message = await asyncio.wait_for(socket.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        logger.warning("No messages for 30 seconds; reconnecting")
                        break
                    handle_message(producer, message)
                    if time.monotonic() - last_log >= LOG_EVERY_SECONDS:
                        last_log = time.monotonic()
                        logger.info(
                            "received=%s delivered=%s rejected=%s failed=%s",
                            Stats.received, Stats.delivered, Stats.rejected, Stats.failed,
                        )
        except (OSError, WebSocketException, asyncio.TimeoutError) as error:
            logger.warning("Connection failed: %s", error)
        if stop.is_set():
            break
        delay = reconnect_delay(attempt)
        attempt += 1
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass


async def main() -> None:
    symbols = [item.strip().upper() for item in os.environ["SYMBOLS"].split(",") if item.strip()]
    producer = Producer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 50,
    })
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(name, stop.set)
    try:
        await stream(producer, symbols, stop)
    finally:
        remaining = producer.flush(15)
        logger.info("Stopped with %s undelivered messages", remaining)


if __name__ == "__main__":
    asyncio.run(main())
