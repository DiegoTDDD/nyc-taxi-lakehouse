"""Dagster definitions: the medallion pipeline as a graph of assets.

bronze -> silver -> gold. Each asset wraps the SQL logic already validated
in the standalone scripts, now with declared dependencies and quality checks.
"""

from datetime import datetime, timezone
from pathlib import Path

import duckdb
from deltalake import write_deltalake
from dagster import asset, asset_check, AssetCheckResult, Definitions, MaterializeResult

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
LAKEHOUSE = PROJECT_ROOT / "data" / "lakehouse"
BRONZE_PATH = LAKEHOUSE / "bronze"
SILVER_PATH = LAKEHOUSE / "silver"
GOLD_DIR = LAKEHOUSE / "gold"
ZONES_CSV = RAW_DIR / "taxi_zone_lookup.csv"


def _delta_scan(path: Path) -> str:
    return f"delta_scan('{path.as_posix()}')"


@asset
def bronze_trips() -> MaterializeResult:
    """Raw TLC Parquet files ingested faithfully into a Delta table."""
    raw_files = sorted(RAW_DIR.glob("yellow_tripdata_*.parquet"))
    total_rows = 0
    for index, parquet_path in enumerate(raw_files):
        con = duckdb.connect()
        ingested_at = datetime.now(timezone.utc)
        arrow_table = con.execute(
            f"""
            SELECT *,
                TIMESTAMP '{ingested_at:%Y-%m-%d %H:%M:%S}' AS _ingested_at,
                '{parquet_path.name}' AS _source_file
            FROM '{parquet_path}'
            """
        ).to_arrow_table()
        con.close()
        write_deltalake(
            str(BRONZE_PATH),
            arrow_table,
            mode="overwrite" if index == 0 else "append",
            schema_mode="overwrite" if index == 0 else "merge",
        )
        total_rows += arrow_table.num_rows
    return MaterializeResult(metadata={"rows": total_rows})


@asset(deps=[bronze_trips])
def silver_trips() -> MaterializeResult:
    """Cleaned, validated, and enriched trips with zone names and congestion era."""
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    bronze = _delta_scan(BRONZE_PATH)
    zones = f"read_csv_auto('{ZONES_CSV.as_posix()}')"
    query = f"""
    WITH cleaned AS (
        SELECT
            VendorID AS vendor_id,
            tpep_pickup_datetime AS pickup_datetime,
            tpep_dropoff_datetime AS dropoff_datetime,
            passenger_count, trip_distance, payment_type,
            PULocationID AS pickup_location_id,
            DOLocationID AS dropoff_location_id,
            fare_amount, tip_amount, tolls_amount, total_amount,
            congestion_surcharge,
            COALESCE(cbd_congestion_fee, 0) AS cbd_congestion_fee,
            _source_file,
            date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_min
        FROM {bronze}
        WHERE fare_amount > 0 AND total_amount > 0
          AND trip_distance > 0 AND trip_distance < 100
          AND passenger_count BETWEEN 1 AND 6
          AND tpep_dropoff_datetime > tpep_pickup_datetime
          AND date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 1 AND 1440
    )
    SELECT c.*,
        EXTRACT(hour FROM c.pickup_datetime) AS pickup_hour,
        dayname(c.pickup_datetime) AS pickup_day_of_week,
        date_trunc('month', c.pickup_datetime) AS pickup_month,
        CASE WHEN c.pickup_datetime >= TIMESTAMP '2025-01-01' THEN 'after' ELSE 'before' END AS congestion_era,
        pu.Zone AS pickup_zone, pu.Borough AS pickup_borough,
        dz.Zone AS dropoff_zone, dz.Borough AS dropoff_borough
    FROM cleaned c
    LEFT JOIN {zones} pu ON c.pickup_location_id = pu.LocationID
    LEFT JOIN {zones} dz ON c.dropoff_location_id = dz.LocationID
    """
    arrow_table = con.execute(query).to_arrow_table()
    con.close()
    write_deltalake(str(SILVER_PATH), arrow_table, mode="overwrite", schema_mode="overwrite")
    return MaterializeResult(metadata={"rows": arrow_table.num_rows})


@asset(deps=[silver_trips])
def gold_tables() -> MaterializeResult:
    """Business-ready aggregate tables built from Silver."""
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    silver = _delta_scan(SILVER_PATH)

    tables = {
        "gold_congestion_impact": f"""
            SELECT congestion_era, COUNT(*) AS trips,
                ROUND(AVG(fare_amount),2) AS avg_fare,
                ROUND(AVG(tip_amount),2) AS avg_tip,
                ROUND(AVG(trip_distance),2) AS avg_distance,
                ROUND(AVG(trip_duration_min),1) AS avg_duration_min,
                ROUND(AVG(cbd_congestion_fee),3) AS avg_congestion_fee
            FROM {silver} WHERE pickup_borough = 'Manhattan'
            GROUP BY congestion_era ORDER BY congestion_era
        """,
        "gold_demand_by_hour": f"""
            SELECT pickup_hour, congestion_era, COUNT(*) AS trips,
                ROUND(AVG(fare_amount),2) AS avg_fare
            FROM {silver} GROUP BY pickup_hour, congestion_era
            ORDER BY pickup_hour, congestion_era
        """,
        "gold_revenue_by_borough": f"""
            SELECT pickup_borough, congestion_era, COUNT(*) AS trips,
                ROUND(SUM(total_amount),2) AS total_revenue,
                ROUND(AVG(total_amount),2) AS avg_revenue_per_trip
            FROM {silver} WHERE pickup_borough IS NOT NULL
            GROUP BY pickup_borough, congestion_era ORDER BY total_revenue DESC
        """,
        "gold_top_routes": f"""
            SELECT pickup_zone, dropoff_zone, COUNT(*) AS trips,
                ROUND(AVG(fare_amount),2) AS avg_fare
            FROM {silver} WHERE pickup_zone IS NOT NULL AND dropoff_zone IS NOT NULL
            GROUP BY pickup_zone, dropoff_zone ORDER BY trips DESC LIMIT 20
        """,
    }
    total = 0
    for name, query in tables.items():
        arrow_table = con.execute(query).to_arrow_table()
        write_deltalake(str(GOLD_DIR / name), arrow_table, mode="overwrite", schema_mode="overwrite")
        total += arrow_table.num_rows
    con.close()
    return MaterializeResult(metadata={"gold_rows_total": total})


@asset_check(asset=silver_trips)
def silver_no_negative_fares() -> AssetCheckResult:
    """Silver must contain no non-positive fares."""
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    bad = con.execute(
        f"SELECT COUNT(*) FROM {_delta_scan(SILVER_PATH)} WHERE fare_amount <= 0"
    ).fetchone()[0]
    con.close()
    return AssetCheckResult(passed=(bad == 0), metadata={"bad_rows": bad})


@asset_check(asset=silver_trips)
def silver_valid_durations() -> AssetCheckResult:
    """Trip durations must be within 1 minute and 24 hours."""
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    bad = con.execute(
        f"SELECT COUNT(*) FROM {_delta_scan(SILVER_PATH)} WHERE trip_duration_min < 1 OR trip_duration_min > 1440"
    ).fetchone()[0]
    con.close()
    return AssetCheckResult(passed=(bad == 0), metadata={"bad_rows": bad})


defs = Definitions(
    assets=[bronze_trips, silver_trips, gold_tables],
    asset_checks=[silver_no_negative_fares, silver_valid_durations],
)
pip show streamlit