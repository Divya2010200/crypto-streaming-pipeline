# Real-Time Crypto Trade Pipeline

A streaming pipeline that moves live public cryptocurrency trades through Kafka and Spark Structured Streaming into PostgreSQL, where they become one-minute price candles.

```
Binance trade stream -> Python producer -> Kafka -> Spark -> PostgreSQL
                              |                      |
                         dead-letter topic      rejected rows
```

## Features

- Reconnects to the live feed with exponential backoff
- Idempotent Kafka producer with keyed messages, partitioned by symbol
- Spark checkpointing and micro-batch processing
- Duplicate-safe storage using a `(symbol, trade_id)` primary key
- Late trades recalculate the affected candle
- Batch metrics, rejected-message storage and a status report
- Docker Compose, automated tests and GitHub Actions

## Run it

Create `.env` containing `POSTGRES_PASSWORD`, then run:

```powershell
docker compose up -d --build
docker compose logs -f producer spark
docker compose --profile tools run --rm report
docker compose --profile tools run --rm tests
```

Spark's interface is available at http://localhost:4040.

## Current limitations

- Runs as a single-node local system
- Kafka uses one broker, so it is not highly available
- Trades are retained in PostgreSQL until removed manually
- Candles are built in PostgreSQL rather than Spark state

## Data

Public market data from the Binance WebSocket trade stream. This project is for data engineering and is not financial advice.
