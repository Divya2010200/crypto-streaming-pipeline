"""Show the current state of the streaming pipeline."""

import os

import psycopg


def main() -> None:
    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        totals = connection.execute(
            """
            SELECT
                (SELECT count(*) FROM trades),
                (SELECT count(*) FROM candles_1m),
                (SELECT count(*) FROM pipeline_batches),
                (SELECT coalesce(sum(duplicate_rows), 0) FROM pipeline_batches),
                (SELECT coalesce(sum(rejected_rows), 0) FROM pipeline_batches),
                (SELECT extract(epoch FROM now() - max(trade_time)) FROM trades)
            """
        ).fetchone()
        print(f"Trades stored: {totals[0]:,}")
        print(f"One-minute candles: {totals[1]:,}")
        print(f"Spark batches: {totals[2]:,}")
        print(f"Duplicate rows ignored: {totals[3]:,}")
        print(f"Rejected rows: {totals[4]:,}")
        if totals[5] is not None:
            print(f"Age of newest trade: {float(totals[5]):.1f} seconds")

        latency = connection.execute(
            """
            SELECT
                percentile_cont(0.5) WITHIN GROUP (ORDER BY feed_seconds),
                percentile_cont(0.95) WITHIN GROUP (ORDER BY feed_seconds),
                percentile_cont(0.5) WITHIN GROUP (ORDER BY pipeline_seconds),
                percentile_cont(0.95) WITHIN GROUP (ORDER BY pipeline_seconds)
            FROM (
                SELECT
                    extract(epoch FROM ingested_at - trade_time)::float8 AS feed_seconds,
                    extract(epoch FROM stored_at - ingested_at)::float8 AS pipeline_seconds
                FROM trades
                WHERE stored_at > now() - interval '5 minutes'
            ) AS recent
            """
        ).fetchone()
        if latency[0] is not None:
            print(f"Exchange to producer, last 5 minutes: median {latency[0]:.1f}s, p95 {latency[1]:.1f}s")
            print(f"Producer to database, last 5 minutes: median {latency[2]:.1f}s, p95 {latency[3]:.1f}s")

        pace = connection.execute(
            """
            SELECT avg(gap_seconds), avg(input_rows)
            FROM (
                SELECT
                    input_rows,
                    extract(epoch FROM processed_at - lag(processed_at) OVER (ORDER BY batch_id))::float8 AS gap_seconds
                FROM pipeline_batches
                ORDER BY batch_id DESC
                LIMIT 20
            ) AS recent
            """
        ).fetchone()
        if pace[0] is not None:
            print(f"Recent Spark batches: one every {pace[0]:.1f}s, {float(pace[1]):.0f} rows on average")

        print("\nLatest complete candles:")
        rows = connection.execute(
            """
            SELECT DISTINCT ON (symbol) symbol, minute, open, high, low, close, volume, trades
            FROM candles_1m
            WHERE minute < date_trunc('minute', now())
            ORDER BY symbol, minute DESC
            """
        ).fetchall()
        for row in rows:
            print(
                f"{row[0]} {row[1]:%H:%M} UTC open={row[2]:.2f} high={row[3]:.2f} "
                f"low={row[4]:.2f} close={row[5]:.2f} volume={row[6]:.4f} trades={row[7]}"
            )


if __name__ == "__main__":
    main()
