"""
streaming_job.py
----------------
Spark Structured Streaming job that:
  1. Reads JSON events from the Kafka topic 'taxi-events'
  2. Parses and casts each field to the correct type
  3. Applies a 5-minute sliding window with a 1-minute slide interval
  4. Aggregates trip count, average fare, total fare, and average distance
     grouped by window and pickup location
  5. Writes results to PostgreSQL using foreachBatch with upsert (ON CONFLICT DO UPDATE)
     to correctly handle re-emitted windows in 'update' output mode.

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS   e.g. kafka:29092
  KAFKA_TOPIC               e.g. taxi-events
  POSTGRES_URL              e.g. jdbc:postgresql://postgres:5432/taxi_pipeline
  POSTGRES_USER
  POSTGRES_PASSWORD
  CHECKPOINT_DIR            e.g. /tmp/spark-checkpoint
"""

import os
import logging
import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────
KAFKA_SERVERS  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC    = os.environ.get("KAFKA_TOPIC", "taxi-events")
PG_URL         = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/taxi_pipeline")
PG_USER        = os.environ.get("POSTGRES_USER", "pipeline")
PG_PASSWORD    = os.environ.get("POSTGRES_PASSWORD", "pipeline123")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/spark-checkpoint")

# Derive host/port/dbname from the JDBC URL for psycopg2
# Expected format: jdbc:postgresql://<host>:<port>/<dbname>
_pg_netloc = PG_URL.replace("jdbc:postgresql://", "")
_pg_host_port, PG_DB = _pg_netloc.rsplit("/", 1)
PG_HOST, PG_PORT = (_pg_host_port.split(":") + ["5432"])[:2]

# ── Schema — aligned with NYC Taxi Trip dataset columns ──────────────────
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

# ── Upsert SQL ────────────────────────────────────────────────────────────
UPSERT_SQL = """
    INSERT INTO taxi_aggregates
        (window_start, window_end, pickup_location,
         trip_count, avg_fare, total_fare, avg_trip_distance)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (window_start, window_end, pickup_location)
    DO UPDATE SET
        trip_count        = EXCLUDED.trip_count,
        avg_fare          = EXCLUDED.avg_fare,
        total_fare        = EXCLUDED.total_fare,
        avg_trip_distance = EXCLUDED.avg_trip_distance,
        processed_at      = NOW();
"""


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TaxiRealTimeStreaming")
        # BUG 4 FIX: removed redundant session-level checkpointLocation here;
        # the query-level .option("checkpointLocation", ...) below is the
        # canonical location and takes precedence anyway.
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def write_to_postgres(batch_df: DataFrame, batch_id: int):
    """foreachBatch sink — writes each micro-batch to PostgreSQL."""
    if batch_df.isEmpty():
        return
    # Drop rows where pickup_location is null to avoid constraint violations
    clean_df = batch_df.filter(batch_df.pickup_location.isNotNull())
    if clean_df.isEmpty():
        return
    logger.info("Writing batch %d (%d rows) to PostgreSQL", batch_id, clean_df.count())
    (
        clean_df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "taxi_aggregates")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading from Kafka topic '%s' at %s", KAFKA_TOPIC, KAFKA_SERVERS)

    # ── 1. Read from Kafka ────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── 2. Parse JSON payload ─────────────────────────────────────────────
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_timestamp")
        .select(
            F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data"),
            F.col("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
    )

    # Cast pickup datetime string to timestamp for windowing
    events = (
        parsed
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("tpep_pickup_datetime"))
        )
        .withColumn(
            "pickup_location",
            F.concat(
                F.round(F.col("pickup_latitude"), 2).cast(StringType()),
                F.lit(","),
                F.round(F.col("pickup_longitude"), 2).cast(StringType())
            )
        )
        .filter(F.col("event_time").isNotNull())
        # Watermark: tolerate events up to 10 minutes late
        .withWatermark("event_time", "10 minutes")
    )

    # ── 3. Sliding window aggregation ────────────────────────────────────
    # 5-minute window, sliding every 1 minute, grouped by pickup location
    aggregated = (
        events
        .groupBy(
            F.window(F.col("event_time"), "5 minutes", "1 minute"),
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
            F.round(F.col("avg_fare"), 2).alias("avg_fare"),
            F.round(F.col("total_fare"), 2).alias("total_fare"),
            F.round(F.col("avg_trip_distance"), 4).alias("avg_trip_distance"),
        )
    )

    # ── 4. Write to PostgreSQL via foreachBatch (upsert) ─────────────────
    query = (
        aggregated.writeStream
        .outputMode("update")
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", CHECKPOINT_DIR + "/taxi_aggregates")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
