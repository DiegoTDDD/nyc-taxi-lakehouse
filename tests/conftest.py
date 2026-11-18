"""A tiny lakehouse: two raw TLC-shaped files (Dec 2024, Jan 2025) and a 4-zone lookup."""

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src" / "nyc_taxi_lakehouse"))


def trip(pickup: datetime, minutes: float = 15, **overrides) -> dict:
    """One raw TLC record; overrides replace fields (use TLC column names)."""
    row = {
        "VendorID": 1,
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + timedelta(minutes=minutes),
        "passenger_count": 1.0,
        "trip_distance": 2.0,
        "PULocationID": 1,
        "DOLocationID": 2,
        "payment_type": 1,
        "fare_amount": 15.0,
        "tip_amount": 3.0,
        "tolls_amount": 0.0,
        "total_amount": 20.0,
        "congestion_surcharge": 2.5,
    }
    row.update(overrides)
    return row


DEC = datetime(2024, 12, 10, 8, 30)
JAN = datetime(2025, 1, 10, 18, 0)

# December 2024: 3 valid trips (2 from Manhattan) and 4 that Silver must drop.
DEC_ROWS = [
    trip(DEC),
    trip(DEC, fare_amount=25.0, total_amount=30.0),
    trip(DEC, PULocationID=3, DOLocationID=1),  # Queens pickup
    trip(DEC, fare_amount=0.0),  # non-positive fare
    trip(DEC, trip_distance=150.0),  # absurd distance
    trip(DEC, passenger_count=0.0),  # no passengers
    trip(DEC, minutes=-5),  # dropoff before pickup
]
# January 2025: the new cbd_congestion_fee column; 2 valid trips and 1 to drop.
JAN_ROWS = [
    {**trip(JAN, minutes=10), "cbd_congestion_fee": 0.75},
    {**trip(JAN, minutes=12, PULocationID=99), "cbd_congestion_fee": 0.75},  # unknown zone
    {**trip(JAN, minutes=2000), "cbd_congestion_fee": 0.75},  # longer than a day
]
ZONES = pd.DataFrame({
    "LocationID": [1, 2, 3, 4],
    "Borough": ["Manhattan", "Manhattan", "Queens", "Brooklyn"],
    "Zone": ["Midtown", "Upper East Side", "Astoria", "Park Slope"],
    "service_zone": ["Yellow Zone", "Yellow Zone", "Boro Zone", "Boro Zone"],
})


@pytest.fixture
def paths(tmp_path):
    from pipeline import LakehousePaths

    p = LakehousePaths(tmp_path)
    p.raw_dir.mkdir(parents=True)
    pd.DataFrame(DEC_ROWS).to_parquet(p.raw_dir / "yellow_tripdata_2024-12.parquet", index=False)
    pd.DataFrame(JAN_ROWS).to_parquet(p.raw_dir / "yellow_tripdata_2025-01.parquet", index=False)
    ZONES.to_csv(p.zones_csv, index=False)
    return p
