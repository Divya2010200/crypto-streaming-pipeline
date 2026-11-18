"""Turn exchange trade messages into the pipeline's event format."""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation

MIN_TIME_MS = 1_500_000_000_000
MAX_FUTURE_MS = 86_400_000


class InvalidTrade(ValueError):
    pass


def _positive_decimal(value, name: str) -> str:
    try:
        number = Decimal(str(value))
    except InvalidOperation as error:
        raise InvalidTrade(f"{name} is not a number") from error
    if not number.is_finite() or number <= 0:
        raise InvalidTrade(f"{name} must be positive")
    return format(number, "f")


def normalize_trade(message: str | bytes, ingested_at_ms: int) -> dict:
    """Validate a direct or combined-stream trade and return a clean event."""
    try:
        data = json.loads(message)
    except (TypeError, ValueError) as error:
        raise InvalidTrade("message is not JSON") from error
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        data = data["data"]
    if not isinstance(data, dict) or data.get("e") != "trade":
        raise InvalidTrade("message is not a trade")

    try:
        symbol = str(data["s"]).upper()
        trade_id = int(data["t"])
        trade_time_ms = int(data["T"])
        is_buyer_maker = data["m"]
        price = _positive_decimal(data["p"], "price")
        quantity = _positive_decimal(data["q"], "quantity")
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, InvalidTrade):
            raise
        raise InvalidTrade("trade has missing or invalid fields") from error

    if not symbol.isalnum() or trade_id < 0 or not isinstance(is_buyer_maker, bool):
        raise InvalidTrade("trade has invalid fields")
    if not MIN_TIME_MS <= trade_time_ms <= ingested_at_ms + MAX_FUTURE_MS:
        raise InvalidTrade("trade time is out of range")
    return {
        "symbol": symbol,
        "trade_id": trade_id,
        "price": price,
        "quantity": quantity,
        "trade_time_ms": trade_time_ms,
        "is_buyer_maker": is_buyer_maker,
        "ingested_at_ms": ingested_at_ms,
    }


def stream_path(symbols: list[str]) -> str:
    streams = "/".join(f"{symbol.lower()}@trade" for symbol in symbols)
    return f"/stream?streams={streams}"


def reconnect_delay(attempt: int, base: float = 1.0, maximum: float = 60.0) -> float:
    return min(maximum, base * 2 ** max(0, attempt))
