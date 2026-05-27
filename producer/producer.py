"""
producer.py
-----------
Reads the NYC Taxi Trip CSV in configurable chunks and replays each row
as a serialised JSON event to a Kafka topic, simulating a real-time
data stream. Chunked reading keeps memory consumption constant regardless
of dataset size.

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS   Broker address          (default: kafka:29092)
  KAFKA_TOPIC               Target topic name       (default: taxi-events)
  DATA_PATH                 Path to CSV file        (default: /data/taxi_data.csv)
  REPLAY_DELAY_MS           Delay between events ms (default: 10)
  CHUNK_SIZE                Rows per CSV chunk      (default: 10000)
"""

import os
import time
import json
import signal
import logging
import pandas as pd
from typing import Iterator
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC:             str = os.environ.get("KAFKA_TOPIC", "taxi-events")
DATA_PATH:         str = os.environ.get("DATA_PATH", "/data/taxi_data.csv")
DELAY_S:         float = int(os.environ.get("REPLAY_DELAY_MS", 10)) / 1000.0
CHUNK_SIZE:        int = int(os.environ.get("CHUNK_SIZE", 10_000))

# ── Graceful shutdown ─────────────────────────────────────────────────────
_shutdown: bool = False

def _handle_signal(signum: int, frame) -> None:
    global _shutdown
    logger.info("Shutdown signal received — finishing current chunk then exiting.")
    _shutdown = True

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def connect_producer(retries: int = 10, backoff: int = 5) -> KafkaProducer:
    """
    Attempt to connect to Kafka with retry logic.
    Raises RuntimeError if all attempts are exhausted.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=5,
                compression_type="gzip",
            )
            logger.info("Connected to Kafka broker at %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Broker unavailable (attempt %d/%d) — retrying in %ds",
                attempt, retries, backoff,
            )
            time.sleep(backoff)
    raise RuntimeError(
        "Failed to connect to Kafka at %s after %d attempts." % (BOOTSTRAP_SERVERS, retries)
    )


def detect_time_column(columns: list) -> str:
    """
    Identify the pickup datetime column by name heuristic.
    Supports both pre-2017 GPS and post-2017 zone-ID dataset schemas.
    """
    candidates = [
        c for c in columns
        if "pickup" in c and ("datetime" in c or "time" in c)
    ]
    if not candidates:
        raise ValueError(
            "Could not detect a pickup datetime column. "
            "Available columns: %s" % columns
        )
    logger.info("Pickup datetime column detected: '%s'", candidates[0])
    return candidates[0]


def serialise_record(row: pd.Series) -> dict:
    """Convert a DataFrame row to a JSON-serialisable dict."""
    record = row.to_dict()
    for key, value in record.items():
        if isinstance(value, pd.Timestamp):
            record[key] = value.isoformat()
        elif pd.isna(value):
            record[key] = None
    return record


def chunk_reader(path: str, chunk_size: int) -> Iterator[pd.DataFrame]:
    """Yield normalised DataFrame chunks from the CSV file."""
    for chunk in pd.read_csv(path, chunksize=chunk_size, low_memory=False):
        chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]
        yield chunk


def produce(producer: KafkaProducer) -> None:
    """
    Stream all rows from the dataset to Kafka.
    Reports throughput metrics after every chunk.
    """
    global _shutdown
    total_sent:  int   = 0
    chunk_num:   int   = 0
    time_col:    str   = ""
    start_time:  float = time.time()

    for chunk in chunk_reader(DATA_PATH, CHUNK_SIZE):
        if _shutdown:
            logger.info("Shutdown flag set — stopping producer cleanly.")
            break

        chunk_num += 1

        if chunk_num == 1:
            time_col = detect_time_column(list(chunk.columns))

        chunk[time_col] = pd.to_datetime(chunk[time_col], errors="coerce")
        chunk = chunk.dropna(subset=[time_col])

        chunk_start = time.time()
        for _, row in chunk.iterrows():
            producer.send(TOPIC, value=serialise_record(row))
            total_sent += 1
            time.sleep(DELAY_S)

        producer.flush()

        elapsed       = time.time() - chunk_start
        throughput    = len(chunk) / elapsed if elapsed > 0 else 0
        total_elapsed = time.time() - start_time

        logger.info(
            "Chunk %d complete | events this chunk: %d | "
            "throughput: %.1f events/s | total sent: %d | elapsed: %.1fs",
            chunk_num, len(chunk), throughput, total_sent, total_elapsed,
        )

    producer.flush()
    logger.info(
        "Producer finished. Total events published to '%s': %d",
        TOPIC, total_sent,
    )


def main() -> None:
    logger.info(
        "Starting producer | topic=%s | chunk_size=%d | delay=%.3fs",
        TOPIC, CHUNK_SIZE, DELAY_S,
    )
    producer = connect_producer()
    produce(producer)
    producer.close()


if __name__ == "__main__":
    main()