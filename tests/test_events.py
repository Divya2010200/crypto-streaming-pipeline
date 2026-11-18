import json

import pytest

from pipeline.events import InvalidTrade, normalize_trade, reconnect_delay, stream_path

NOW = 1_800_000_000_000
TRADE = {"e": "trade", "E": NOW, "s": "btcusdt", "t": 12, "p": "100.50", "q": "0.25", "T": NOW - 50, "m": True, "M": True}


def test_direct_trade_is_normalized():
    event = normalize_trade(json.dumps(TRADE), NOW)
    assert event == {
        "symbol": "BTCUSDT", "trade_id": 12, "price": "100.50", "quantity": "0.25",
        "trade_time_ms": NOW - 50, "is_buyer_maker": True, "ingested_at_ms": NOW,
    }


def test_combined_stream_message_is_accepted():
    message = {"stream": "btcusdt@trade", "data": TRADE}
    assert normalize_trade(json.dumps(message), NOW)["trade_id"] == 12


@pytest.mark.parametrize(
    "change",
    [{"e": "depth"}, {"p": "0"}, {"p": "NaN"}, {"q": "-1"}, {"t": -1}, {"T": 5}, {"T": NOW + 90_000_000}, {"m": "yes"}, {"s": "BTC/USDT"}],
)
def test_invalid_trades_are_rejected(change):
    with pytest.raises(InvalidTrade):
        normalize_trade(json.dumps(TRADE | change), NOW)


@pytest.mark.parametrize("message", ["not json", "[]", "{}"])
def test_invalid_messages_are_rejected(message):
    with pytest.raises(InvalidTrade):
        normalize_trade(message, NOW)


def test_stream_path_and_backoff():
    assert stream_path(["BTCUSDT", "ETHUSDT"]) == "/stream?streams=btcusdt@trade/ethusdt@trade"
    assert [reconnect_delay(i) for i in [0, 1, 2, 10]] == [1, 2, 4, 60]
