"""Read trades from Kafka with Spark and store them in PostgreSQL."""

from __future__ import annotations

import os

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

from pipeline.events import MAX_FUTURE_MS, MIN_TIME_MS
from pipeline.sink import store_batch

TOPIC = "crypto.trades.raw"
TRADE_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("trade_id", LongType()),
    StructField("price", StringType()),
    StructField("quantity", StringType()),
    StructField("trade_time_ms", LongType()),
    StructField("is_buyer_maker", BooleanType()),
    StructField("ingested_at_ms", LongType()),
])


def parse(frame):
    parsed = frame.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("value").cast("string").alias("raw_value"),
        F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("trade"),
    )
    return parsed.select(
        "topic", "partition", "offset", "raw_value",
        F.col("trade.symbol").alias("symbol"),
        F.col("trade.trade_id").alias("trade_id"),
        F.expr("try_cast(trade.price AS DECIMAL(24, 8))").alias("price"),
        F.expr("try_cast(trade.quantity AS DECIMAL(24, 8))").alias("quantity"),
        F.col("trade.trade_time_ms").alias("trade_time_ms"),
        F.col("trade.is_buyer_maker").alias("is_buyer_maker"),
        F.col("trade.ingested_at_ms").alias("ingested_at_ms"),
    )


def valid_condition():
    return (
        F.col("symbol").isNotNull()
        & F.col("trade_id").isNotNull() & (F.col("trade_id") >= 0)
        & F.col("price").isNotNull() & (F.col("price") > 0)
        & F.col("quantity").isNotNull() & (F.col("quantity") > 0)
        & F.col("is_buyer_maker").isNotNull()
        & F.col("ingested_at_ms").isNotNull()
        & F.col("trade_time_ms").between(MIN_TIME_MS, F.col("ingested_at_ms") + MAX_FUTURE_MS)
    )


def main() -> None:
    database_url = os.environ["DATABASE_URL"]
    spark = SparkSession.builder.appName("crypto-trades").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    def process(batch, batch_id: int) -> None:
        checked = parse(batch).withColumn("is_valid", F.coalesce(valid_condition(), F.lit(False))).cache()
        try:
            trades = checked.filter("is_valid").select(
                "symbol", "trade_id", "price", "quantity",
                "trade_time_ms", "is_buyer_maker", "ingested_at_ms",
            )
            rejected = checked.filter("NOT is_valid").select("topic", "partition", "offset", "raw_value")
            with psycopg.connect(database_url) as connection:
                result = store_batch(
                    connection,
                    batch_id,
                    (tuple(row) for row in trades.toLocalIterator()),
                    (tuple(row) for row in rejected.toLocalIterator()),
                )
            print(f"batch={batch_id} {result}", flush=True)
        finally:
            checked.unpersist()

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
    )
    query = (
        source.writeStream.foreachBatch(process)
        .option("checkpointLocation", os.environ["CHECKPOINT_DIR"])
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
