"""Store micro-batches in PostgreSQL so that replays do not change results."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class BatchResult:
    input_rows: int
    valid_rows: int
    inserted_rows: int
    duplicate_rows: int
    rejected_rows: int


MERGE_TRADES_AND_CANDLES = """
WITH inserted AS (
    INSERT INTO trades (symbol, trade_id, price, quantity, trade_time, is_buyer_maker, ingested_at)
    SELECT
        symbol, trade_id, price, quantity,
        to_timestamp(trade_time_ms / 1000.0),
        is_buyer_maker,
        to_timestamp(ingested_at_ms / 1000.0)
    FROM incoming_trades
    ON CONFLICT (symbol, trade_id) DO NOTHING
    RETURNING symbol, date_trunc('minute', trade_time) AS minute
),
affected AS (
    SELECT DISTINCT symbol, minute FROM inserted
),
combined AS (
    SELECT symbol, trade_id, price, quantity, date_trunc('minute', trade_time) AS minute, trade_time
    FROM trades
    WHERE (symbol, date_trunc('minute', trade_time)) IN (SELECT symbol, minute FROM affected)
    UNION ALL
    SELECT
        i.symbol, i.trade_id, i.price, i.quantity,
        date_trunc('minute', to_timestamp(i.trade_time_ms / 1000.0)),
        to_timestamp(i.trade_time_ms / 1000.0)
    FROM incoming_trades AS i
    WHERE NOT EXISTS (
        SELECT 1 FROM trades AS t WHERE t.symbol = i.symbol AND t.trade_id = i.trade_id
    )
),
candles AS (
    SELECT
        symbol,
        minute,
        (array_agg(price ORDER BY trade_time, trade_id))[1] AS open,
        max(price) AS high,
        min(price) AS low,
        (array_agg(price ORDER BY trade_time DESC, trade_id DESC))[1] AS close,
        sum(quantity) AS volume,
        sum(price * quantity) AS quote_volume,
        count(*)::int AS trades
    FROM combined
    GROUP BY symbol, minute
),
saved AS (
    INSERT INTO candles_1m AS c (symbol, minute, open, high, low, close, volume, quote_volume, trades)
    SELECT * FROM candles
    ON CONFLICT (symbol, minute) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        quote_volume = EXCLUDED.quote_volume,
        trades = EXCLUDED.trades,
        updated_at = now()
    RETURNING 1
)
SELECT (SELECT count(*) FROM inserted)::int
"""


def store_batch(connection, batch_id: int, trades: Iterable[tuple], rejected: Iterable[tuple]) -> BatchResult:
    """Write one batch in a single transaction.

    trades: symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker, ingested_at_ms
    rejected: topic, partition, offset, raw_value
    """
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TEMP TABLE incoming_trades (
                    symbol TEXT NOT NULL,
                    trade_id BIGINT NOT NULL,
                    price NUMERIC(24, 8) NOT NULL,
                    quantity NUMERIC(24, 8) NOT NULL,
                    trade_time_ms BIGINT NOT NULL,
                    is_buyer_maker BOOLEAN NOT NULL,
                    ingested_at_ms BIGINT NOT NULL
                ) ON COMMIT DROP
                """
            )
            valid_rows = 0
            with cursor.copy("COPY incoming_trades FROM STDIN") as copy:
                for row in trades:
                    copy.write_row(row)
                    valid_rows += 1

            # Keep one copy of a trade repeated within this batch.
            cursor.execute(
                """
                DELETE FROM incoming_trades AS a
                USING incoming_trades AS b
                WHERE a.ctid > b.ctid AND a.symbol = b.symbol AND a.trade_id = b.trade_id
                """
            )
            inserted = cursor.execute(MERGE_TRADES_AND_CANDLES).fetchone()[0]

            rejected_rows = 0
            for topic, partition, offset, raw_value in rejected:
                cursor.execute(
                    """
                    INSERT INTO rejected_messages
                        (kafka_topic, kafka_partition, kafka_offset, batch_id, raw_value)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    [topic, partition, offset, batch_id, raw_value],
                )
                rejected_rows += 1

            result = BatchResult(
                input_rows=valid_rows + rejected_rows,
                valid_rows=valid_rows,
                inserted_rows=inserted,
                duplicate_rows=valid_rows - inserted,
                rejected_rows=rejected_rows,
            )
            cursor.execute(
                """
                INSERT INTO pipeline_batches
                    (batch_id, input_rows, valid_rows, inserted_rows, duplicate_rows, rejected_rows)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id) DO NOTHING
                """,
                [batch_id, result.input_rows, result.valid_rows, result.inserted_rows,
                 result.duplicate_rows, result.rejected_rows],
            )
    return result
