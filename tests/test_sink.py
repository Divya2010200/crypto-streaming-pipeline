import os
from decimal import Decimal
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from pipeline.sink import store_batch  # noqa: E402

SCHEMA = (Path(__file__).parent.parent / "sql" / "schema.sql").read_text(encoding="utf-8")
MINUTE_MS = 1_800_000_000_000 - (1_800_000_000_000 % 60_000)


def trade(trade_id, price, quantity="1", offset_ms=0, symbol="BTCUSDT"):
    return (symbol, trade_id, Decimal(price), Decimal(quantity), MINUTE_MS + offset_ms, False, MINUTE_MS + 70_000)


@pytest.fixture
def connection():
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL is not set")
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute(SCHEMA)
        conn.execute("TRUNCATE trades, candles_1m, pipeline_batches, rejected_messages")
        yield conn


def candle(connection):
    return connection.execute(
        "SELECT open, high, low, close, volume, quote_volume, trades FROM candles_1m"
    ).fetchone()


def test_batch_builds_candle(connection):
    result = store_batch(connection, 1, [trade(2, "110", "2", 2000), trade(1, "100", "1", 1000), trade(3, "90", "1", 3000)], [])
    assert result.inserted_rows == 3
    assert candle(connection) == (Decimal("100"), Decimal("110"), Decimal("90"), Decimal("90"), Decimal("4"), Decimal("410"), 3)


def test_replaying_a_batch_changes_nothing(connection):
    rows = [trade(1, "100"), trade(2, "120", offset_ms=1000)]
    store_batch(connection, 1, rows, [])
    before = candle(connection)
    result = store_batch(connection, 1, rows, [])
    assert (result.inserted_rows, result.duplicate_rows) == (0, 2)
    assert candle(connection) == before
    assert connection.execute("SELECT count(*) FROM trades").fetchone()[0] == 2


def test_late_trade_updates_existing_candle(connection):
    store_batch(connection, 1, [trade(2, "120", offset_ms=2000)], [])
    store_batch(connection, 2, [trade(1, "100", offset_ms=1000)], [])
    assert candle(connection)[:4] == (Decimal("100"), Decimal("120"), Decimal("100"), Decimal("120"))


def test_duplicates_inside_a_batch_are_counted_once(connection):
    result = store_batch(connection, 1, [trade(1, "100"), trade(1, "100")], [])
    assert (result.valid_rows, result.inserted_rows, result.duplicate_rows) == (2, 1, 1)
    assert candle(connection)[-1] == 1


def test_rejected_messages_are_kept_once(connection):
    bad = [("crypto.trades.raw", 0, 5, "bad")]
    assert store_batch(connection, 1, [], bad).rejected_rows == 1
    store_batch(connection, 2, [], bad)
    assert connection.execute("SELECT count(*) FROM rejected_messages").fetchone()[0] == 1
