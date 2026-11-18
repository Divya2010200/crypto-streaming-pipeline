CREATE TABLE IF NOT EXISTS trades (
    symbol TEXT NOT NULL,
    trade_id BIGINT NOT NULL,
    price NUMERIC(24, 8) NOT NULL CHECK (price > 0),
    quantity NUMERIC(24, 8) NOT NULL CHECK (quantity > 0),
    trade_time TIMESTAMPTZ NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    stored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades (symbol, trade_time);

CREATE TABLE IF NOT EXISTS candles_1m (
    symbol TEXT NOT NULL,
    minute TIMESTAMPTZ NOT NULL,
    open NUMERIC(24, 8) NOT NULL,
    high NUMERIC(24, 8) NOT NULL,
    low NUMERIC(24, 8) NOT NULL,
    close NUMERIC(24, 8) NOT NULL,
    volume NUMERIC(32, 8) NOT NULL,
    quote_volume NUMERIC(40, 8) NOT NULL,
    trades INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, minute)
);

CREATE TABLE IF NOT EXISTS pipeline_batches (
    batch_id BIGINT PRIMARY KEY,
    input_rows INTEGER NOT NULL,
    valid_rows INTEGER NOT NULL,
    inserted_rows INTEGER NOT NULL,
    duplicate_rows INTEGER NOT NULL,
    rejected_rows INTEGER NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rejected_messages (
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    batch_id BIGINT NOT NULL,
    raw_value TEXT,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);
