CREATE TABLE IF NOT EXISTS taxi_aggregates (
    window_start      TIMESTAMP NOT NULL,
    window_end        TIMESTAMP NOT NULL,
    pickup_location   VARCHAR(100),
    trip_count        BIGINT,
    avg_fare          NUMERIC(10, 2),
    total_fare        NUMERIC(12, 2),
    avg_trip_distance NUMERIC(8, 4),
    processed_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS taxi_events_raw (
    id               SERIAL PRIMARY KEY,
    event_time       TIMESTAMP,
    pickup_location  VARCHAR(100),
    dropoff_location VARCHAR(100),
    passenger_count  INT,
    trip_distance    NUMERIC(8, 4),
    fare_amount      NUMERIC(10, 2),
    ingested_at      TIMESTAMP DEFAULT NOW()
);