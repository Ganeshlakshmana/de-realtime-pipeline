# de-realtime-pipeline

**Data Engineering — DLMDSEDE02 | Task 2 — Real-Time Streaming Pipeline**
IU Internationale Hochschule | Ganesh Lakshmana | Matriculation: 10241551

---

## Overview

A fully containerised real-time data pipeline built with Apache Kafka, Apache Spark Structured Streaming, and PostgreSQL. The system ingests NYC Taxi Trip data, processes it as a continuous event stream, applies sliding window aggregations, and delivers results to a PostgreSQL sink — all orchestrated via Docker Compose as Infrastructure as Code.

---

## Architecture

```
NYC Taxi CSV
     │
     ▼
Python Producer  ──(publish)──►  Apache Kafka  ──(consume)──►  Spark Structured Streaming
                                  (taxi-events)                      │
                                  Zookeeper                          ▼
                                                               PostgreSQL Sink
                                                                     │
                                                                     ▼
                                                            Reporting Application
                                                              (out of scope)
```

All services run inside a Docker bridge network (`kafka-net`) with no unintended external exposure.

---

## Services

| Service    | Image                            | Role                                      |
|------------|----------------------------------|-------------------------------------------|
| zookeeper  | confluentinc/cp-zookeeper:7.5.0  | Kafka broker coordination                 |
| kafka      | confluentinc/cp-kafka:7.5.0      | Message broker — topic: `taxi-events`     |
| postgres   | postgres:15-alpine               | Aggregated results sink                   |
| producer   | custom (python:3.11-slim)        | Replays CSV as a real-time event stream   |
| spark      | custom (bitnami/spark:3.5.0)     | Structured Streaming + window aggregation |

---

## Prerequisites

- Docker Desktop (Windows) with WSL2 backend enabled
- At least 8 GB RAM allocated to Docker
- Git

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/de-realtime-pipeline.git
cd de-realtime-pipeline
```

### 2. Download the NYC Taxi dataset

1. Go to [https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
2. Download any of the CSV files (e.g. `yellow_tripdata_2016-03.csv`)
3. Place the file inside the `data/` directory and rename it to `taxi_data.csv`

```
de-realtime-pipeline/
└── data/
    └── taxi_data.csv   ← place it here
```

### 3. Build and start all services

```bash
docker compose up --build
```

This will:
- Start Zookeeper and Kafka
- Initialise the PostgreSQL database with the schema
- Build and start the Python producer (begins streaming events to Kafka)
- Build and start the Spark job (begins consuming and aggregating)

### 4. Verify the pipeline

Check aggregated results in PostgreSQL:

```bash
docker exec -it postgres psql -U pipeline -d taxi_pipeline -c \
  "SELECT * FROM taxi_aggregates ORDER BY window_start DESC LIMIT 20;"
```

---

## Project Structure

```
de-realtime-pipeline/
├── docker-compose.yml          # IaC — full stack definition
├── data/                       # Place taxi_data.csv here (gitignored)
├── postgres/
│   └── init.sql                # Schema initialisation
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py             # Kafka event producer
└── spark/
    ├── Dockerfile
    ├── requirements.txt
    └── streaming_job.py        # Spark Structured Streaming job
```

---

## Stopping the pipeline

```bash
docker compose down
```

To also remove volumes (clears PostgreSQL data and Spark checkpoints):

```bash
docker compose down -v
```

---

## Reliability & Scalability Notes

- Kafka `acks=all` on the producer ensures no message loss on broker failure
- Spark checkpointing to a Docker volume enables exactly-once recovery
- Watermarking (`10 minutes`) handles late-arriving events gracefully
- The Docker Compose file can be extended with additional Kafka brokers or Spark workers by replicating the respective service blocks

---

## Data Security Notes

- All inter-service traffic is confined to the internal `kafka-net` Docker bridge network
- PostgreSQL credentials are passed via environment variables (use Docker secrets in production)
- No services are exposed externally beyond ports needed for local development access
