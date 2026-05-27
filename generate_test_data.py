"""
generate_test_data.py
---------------------
Generates a synthetic NYC Taxi Trip CSV (50 000 rows) that matches
the exact column schema expected by producer.py and streaming_job.py.

Run once before `docker compose up --build`:
    python generate_test_data.py
"""

import csv
import random
from datetime import datetime, timedelta
import os

random.seed(42)

HEADERS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID",
    "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
]

ZONES = list(range(1, 266))          # 265 real NYC taxi zone IDs
BASE  = datetime(2016, 2, 1, 0, 0, 0)
N     = 50_000

rows = []
for _ in range(N):
    pickup_dt  = BASE + timedelta(seconds=random.randint(0, 28 * 24 * 3600))
    dropoff_dt = pickup_dt + timedelta(minutes=random.randint(3, 60))
    distance   = round(random.uniform(0.5, 25.0), 2)
    fare       = round(2.5 + distance * random.uniform(1.8, 3.5), 2)
    tip        = round(fare * random.uniform(0.0, 0.30), 2)
    total      = round(fare + tip + 0.5 + 0.5 + 0.3, 2)
    rows.append([
        random.randint(1, 2),
        pickup_dt.strftime("%Y-%m-%d %H:%M:%S"),
        dropoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
        random.randint(1, 6),
        distance,
        1,
        "N",
        random.choice(ZONES),
        random.choice(ZONES),
        random.randint(1, 4),
        fare, 0.5, 0.5, tip, 0.0, 0.3, total,
    ])

out = os.path.join(os.path.dirname(__file__), "data", "taxi_data.csv")
os.makedirs(os.path.dirname(out), exist_ok=True)

with open(out, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(HEADERS)
    writer.writerows(rows)

print(f"✓ Generated {N:,} rows  →  {out}")
