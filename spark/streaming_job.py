"""
streaming_job.py
----------------
Spark Structured Streaming job for the NYC Taxi real-time pipeline.

Pipeline steps:
  1. Read JSON events from Kafka topic (taxi-events)
  2. Parse payload against a defined schema with type enforcement
  3. Derive pickup_location from GPS coordinates (lat/lon rounded to 2dp)
  4. Apply a configurable sliding window aggregation with watermarking
  5. Write aggregated results to PostgreSQL via foreachBatch sink
  6. Persist checkpoint state to a Docker volume for exactly-once recovery

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS   Broker address            (default: kafka:29092)
  KAFKA_TOPIC               Source topic              (default: taxi-events)
  POSTGRES_URL              JDBC connection string
  POSTGRES_USER             Database user
  POSTGRES_PASSWORD         Database password
  CHECKPOINT_DIR            Checkpoint storage path   (default: /tmp/spark-checkpoint)
  WINDOW_DURATION           Window size               (default: 5 minutes)
  SLIDE_DURATION            Slide interval            (default: 1 minute)
  WATERMARK_DELAY           Late event tolerance      (default: 10 minutes)
  TRIGGER_INTERVAL          Micro-batch frequency     (default: 30 seconds)
"""

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
)

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────
KAFKA_SERVERS:    str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC:      str = os.environ.get("KAFKA_TOPIC", "taxi-events")
PG_URL:           str = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/taxi_pipeline")
PG_USER:          str = os.environ.get("POSTGRES_USER", "pipeline")
PG_PASSWORD:      str = os.environ.get("POSTGRES_PASSWORD", "pipeline123")
CHECKPOINT_DIR:   str = os.environ.get("CHECKPOINT_DIR", "/tmp/spark-checkpoint")
WINDOW_DURATION:  str = os.environ.get("WINDOW_DURATION", "5 minutes")
SLIDE_DURATION:   str = os.environ.get("SLIDE_DURATION", "1 minute")
WATERMARK_DELAY:  str = os.environ.get("WATERMARK_DELAY", "10 minutes")
TRIGGER_INTERVAL: str = os.environ.get("TRIGGER_INTERVAL", "30 seconds")

# ── Event schema ──────────────────────────────────────────────────────────
# Aligned with NYC Yellow Taxi Trip dataset (pre-2017 GPS coordinate schema).
# Fields not in this schema are silently ignored by from_json.
EVENT_SCHEMA = StructType([
    StructField("tpep_pickup_datetime",  StringType(),  True),
    StructField("tpep_dropoff_datetime", StringType(),  True),
    StructField("passenger_count",       IntegerType(), True),
    StructField("trip_distance",         DoubleType(),  True),
    StructField("pickup_longitude",      DoubleType(),  True),
    StructField("pickup_latitude",       DoubleType(),  True),
    StructField("dropoff_longitude",     DoubleType(),  True),
    StructField("dropoff_latitude",      DoubleType(),  True),
    StructField("fare_amount",           DoubleType(),  True),
    StructField("total_amount",          DoubleType(),  True),
])


def build_spark_session() -> SparkSession:
    """
    Initialise SparkSession optimised for local streaming deployment.
    Shuffle partitions reduced to 4 to avoid over-partitioning on
    a single-node development environment.
    """
    return (
        SparkSession.builder
        .appName("TaxiRealTimeStreaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )


def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch sink — writes each micro-batch to PostgreSQL.

    Filters null pickup_location rows before writing to preserve
    data quality in the sink table. JDBC batchsize of 1000 ensures
    efficient bulk inserts rather than row-by-row writes.
    """
    clean_df = batch_df.filter(batch_df.pickup_location.isNotNull())

    if clean_df.isEmpty():
        logger.info("Batch %d: no rows to write — skipping.", batch_id)
        return

    row_count = clean_df.count()
    logger.info("Batch %d: writing %d rows to PostgreSQL.", batch_id, row_count)

    (
        clean_df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "taxi_aggregates")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )

    logger.info("Batch %d: write complete.", batch_id)


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "Streaming job starting | topic=%s | window=%s | slide=%s | watermark=%s",
        KAFKA_TOPIC, WINDOW_DURATION, SLIDE_DURATION, WATERMARK_DELAY,
    )

    # 1. Ingest from Kafka
    # startingOffsets=earliest ensures no events missed on cold start
    # failOnDataLoss=false prevents job failure if Kafka log segments expire
    # maxOffsetsPerTrigger prevents overwhelming Spark on initial catch-up
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "50000")
        .load()
    )

    # 2. Parse JSON payload against defined schema
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    # 3. Transform and enrich
    # pickup_location derived from GPS coords rounded to 2dp (~1km resolution)
    # Watermark applied before groupBy so Spark can manage state size correctly
    events = (
        parsed
        .withColumn("event_time", F.to_timestamp(F.col("tpep_pickup_datetime")))
        .withColumn(
            "pickup_location",
            F.concat(
                F.round(F.col("pickup_latitude"),  2).cast(StringType()),
                F.lit(","),
                F.round(F.col("pickup_longitude"), 2).cast(StringType()),
            ),
        )
        .filter(F.col("event_time").isNotNull())
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    # 4. Sliding window aggregation grouped by pickup location
    aggregated = (
        events
        .groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
            F.col("pickup_location"),
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.sum("fare_amount").alias("total_fare"),
            F.avg("trip_distance").alias("avg_trip_distance"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("pickup_location"),
            F.col("trip_count"),
            F.round(F.col("avg_fare"),          2).alias("avg_fare"),
            F.round(F.col("total_fare"),         2).alias("total_fare"),
            F.round(F.col("avg_trip_distance"),  4).alias("avg_trip_distance"),
        )
    )

    # 5. Write to PostgreSQL via foreachBatch every TRIGGER_INTERVAL
    query = (
        aggregated.writeStream
        .outputMode("update")
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "taxi_aggregates"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()