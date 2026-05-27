"""
producer.py
-----------
Reads the NYC Taxi Trip CSV in chunks and replays each row as a JSON
event to a Kafka topic, simulating a real-time data stream.
Chunked reading prevents out-of-memory errors on large files.
"""

import os
import time
import json
import logging
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC             = os.environ.get("KAFKA_TOPIC", "taxi-events")
DATA_PATH         = os.environ.get("DATA_PATH", "/data/taxi_data.csv")
DELAY_S           = int(os.environ.get("REPLAY_DELAY_MS", 10)) / 1000.0
CHUNK_SIZE        = 10_000  # rows per chunk — keeps memory usage low


def wait_for_kafka(retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=5,
            )
            logger.info("Connected to Kafka at %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka not ready (attempt %d/%d). Retrying in %ds...",
                attempt, retries, delay,
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after %d attempts." % retries)


def get_time_column(columns):
    candidates = [
        c for c in columns
        if "pickup" in c and ("datetime" in c or "time" in c)
    ]
    if not candidates:
        raise ValueError("No pickup datetime column found. Columns: %s" % list(columns))
    return candidates[0]


def produce(producer: KafkaProducer):
    total_sent = 0
    chunk_num  = 0

    for chunk in pd.read_csv(DATA_PATH, chunksize=CHUNK_SIZE, low_memory=False):
        chunk_num += 1

        # Normalise column names on first chunk
        chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]

        if chunk_num == 1:
            time_col = get_time_column(chunk.columns)
            logger.info("Time column detected: %s", time_col)

        chunk[time_col] = pd.to_datetime(chunk[time_col], errors="coerce")
        chunk = chunk.dropna(subset=[time_col])

        for _, row in chunk.iterrows():
            record = row.to_dict()
            for k, v in record.items():
                if isinstance(v, pd.Timestamp):
                    record[k] = v.isoformat()
                elif pd.isna(v):
                    record[k] = None

            producer.send(TOPIC, value=record)
            total_sent += 1
            time.sleep(DELAY_S)

        producer.flush()
        logger.info("Chunk %d sent — total events so far: %d", chunk_num, total_sent)

    logger.info("All %d events published to topic '%s'.", total_sent, TOPIC)


def main():
    producer = wait_for_kafka()
    logger.info("Starting chunked event replay to topic '%s'...", TOPIC)
    produce(producer)


if __name__ == "__main__":
    main()