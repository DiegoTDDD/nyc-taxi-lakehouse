"""Silver layer: clean, validate, and enrich the Bronze data.

Removes invalid trips, joins taxi zone names, derives analytical columns,
and standardizes the congestion fee column across the 2024/2025 schemas.
"""

from pathlib import Path

import duckdb
from deltalake import write_deltalake

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_PATH = PROJECT_ROOT / "data" / "lakehouse" / "bronze"
SILVER_PATH = PROJECT_ROOT / "data" / "lakehouse" / "silver"
ZONES_CSV = PROJECT_ROOT / "data" / "raw" / "taxi_zone_lookup.csv"


def build_silver() -> int:
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    bronze = f"delta_scan('{BRONZE_PATH.as_posix()}')"
    zones = f"read_csv_auto('{ZONES_CSV.as_posix()}')"

    query = f"""
    WITH cleaned AS (
        SELECT
            VendorID                        AS vendor_id,
            tpep_pickup_datetime            AS pickup_datetime,
            tpep_dropoff_datetime           AS dropoff_datetime,
            passenger_count,
            trip_distance,
            payment_type,
            PULocationID                    AS pickup_location_id,
            DOLocationID                    AS dropoff_location_id,
            fare_amount,
            tip_amount,
            tolls_amount,
            total_amount,
            congestion_surcharge,
            COALESCE(cbd_congestion_fee, 0) AS cbd_congestion_fee,
            _source_file,
            date_diff(
                'minute', tpep_pickup_datetime, tpep_dropoff_datetime
            )                               AS trip_duration_min
        FROM {bronze}
        WHERE fare_amount > 0
          AND total_amount > 0
          AND trip_distance > 0
          AND trip_distance < 100
          AND passenger_count BETWEEN 1 AND 6
          AND tpep_dropoff_datetime > tpep_pickup_datetime
          AND date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 1 AND 1440
    )
    SELECT
        c.*,
        EXTRACT(hour FROM c.pickup_datetime)  AS pickup_hour,
        dayname(c.pickup_datetime)            AS pickup_day_of_week,
        date_trunc('month', c.pickup_datetime) AS pickup_month,
        CASE
            WHEN c.pickup_datetime >= TIMESTAMP '2025-01-01' THEN 'after'
            ELSE 'before'
        END                                   AS congestion_era,
        pu.Zone                               AS pickup_zone,
        pu.Borough                            AS pickup_borough,
        dz.Zone                               AS dropoff_zone,
        dz.Borough                            AS dropoff_borough
    FROM cleaned c
    LEFT JOIN {zones} pu ON c.pickup_location_id = pu.LocationID
    LEFT JOIN {zones} dz ON c.dropoff_location_id = dz.LocationID
    """

    arrow_table = con.execute(query).to_arrow_table()
    con.close()

    write_deltalake(str(SILVER_PATH), arrow_table, mode="overwrite", schema_mode="overwrite")
    return arrow_table.num_rows


def main() -> None:
    SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    print("[silver] building silver layer from bronze ...")
    rows = build_silver()
    print(f"[silver] complete: {rows:,} clean rows written to {SILVER_PATH}")


if __name__ == "__main__":
    main()