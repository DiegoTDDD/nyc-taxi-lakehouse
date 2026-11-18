"""The medallion pipeline: Bronze -> Silver -> Gold, and the data-quality measurements.

This is the only implementation. The Dagster assets (definitions.py) and the
standalone scripts (bronze.py, silver.py, gold.py) both call it, and
tests/test_pipeline.py runs it end to end on a few hand-made rows.

Delta tables are read with the `deltalake` package and handed to DuckDB as Arrow
datasets, so DuckDB does not need to download its `delta` extension at run time
(ADR 0003).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from deltalake import DeltaTable, write_deltalake

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONGESTION_PRICING_START = "2025-01-01"
MAX_REMOVED_SHARE = 0.30  # Silver may drop at most 30% of Bronze as invalid (it drops ~19%)


@dataclass(frozen=True)
class LakehousePaths:
    """Where each layer lives, under one data folder (data/ by default, a temp folder in tests)."""

    data_dir: Path = PROJECT_ROOT / "data"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def zones_csv(self) -> Path:
        return self.raw_dir / "taxi_zone_lookup.csv"

    @property
    def bronze(self) -> Path:
        return self.data_dir / "lakehouse" / "bronze"

    @property
    def silver(self) -> Path:
        return self.data_dir / "lakehouse" / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "lakehouse" / "gold"

    def gold(self, name: str) -> Path:
        return self.gold_dir / name


def connect(**tables: Path) -> duckdb.DuckDBPyConnection:
    """DuckDB connection with each Delta table registered as a view under the given name."""
    con = duckdb.connect()
    for name, path in tables.items():
        con.register(name, DeltaTable(str(path)).to_pyarrow_dataset())
    return con


# --------------------------------------------------------------------------- Bronze


def ingest_bronze(paths: LakehousePaths) -> int:
    """Append every raw TLC file to the Bronze Delta table as is, plus ingestion metadata.

    The first file overwrites the table; later files are merged, so the table's
    schema grows when the 2025 files add cbd_congestion_fee (2024 rows stay null).
    """
    raw_files = sorted(paths.raw_dir.glob("yellow_tripdata_*.parquet"))
    if not raw_files:
        raise FileNotFoundError(f"no yellow_tripdata_*.parquet in {paths.raw_dir}")
    paths.bronze.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    for index, parquet_path in enumerate(raw_files):
        ingested_at = datetime.now(UTC)
        con = duckdb.connect()
        table = con.sql(
            f"""
            SELECT *,
                TIMESTAMP '{ingested_at:%Y-%m-%d %H:%M:%S}' AS _ingested_at,
                '{parquet_path.name}' AS _source_file
            FROM '{parquet_path.as_posix()}'
            """
        ).to_arrow_table()
        con.close()
        write_deltalake(
            str(paths.bronze),
            table,
            mode="overwrite" if index == 0 else "append",
            schema_mode="overwrite" if index == 0 else "merge",
        )
        log.info("[bronze] %s: %s rows", parquet_path.name, f"{table.num_rows:,}")
        total += table.num_rows
    return total


# --------------------------------------------------------------------------- Silver

# Rules a trip must meet to reach Silver. Each one removes a clear data-entry error.
SILVER_RULES = """
    fare_amount > 0
    AND total_amount > 0
    AND trip_distance > 0
    AND trip_distance < 100
    AND passenger_count BETWEEN 1 AND 6
    AND tpep_dropoff_datetime > tpep_pickup_datetime
    AND date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 1 AND 1440
"""

SILVER_SQL = f"""
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
        date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_min
    FROM bronze
    WHERE {SILVER_RULES}
)
SELECT
    c.*,
    EXTRACT(hour FROM c.pickup_datetime)   AS pickup_hour,
    dayname(c.pickup_datetime)             AS pickup_day_of_week,
    date_trunc('month', c.pickup_datetime) AS pickup_month,
    CASE
        WHEN c.pickup_datetime >= TIMESTAMP '{CONGESTION_PRICING_START}' THEN 'after'
        ELSE 'before'
    END                                    AS congestion_era,
    pu.Zone                                AS pickup_zone,
    pu.Borough                             AS pickup_borough,
    dz.Zone                                AS dropoff_zone,
    dz.Borough                             AS dropoff_borough
FROM cleaned c
LEFT JOIN zones pu ON c.pickup_location_id = pu.LocationID
LEFT JOIN zones dz ON c.dropoff_location_id = dz.LocationID
"""


def build_silver(paths: LakehousePaths) -> int:
    """Clean and enrich Bronze into the Silver Delta table; return its row count."""
    con = connect(bronze=paths.bronze)
    con.execute(f"CREATE VIEW zones AS SELECT * FROM read_csv_auto('{paths.zones_csv.as_posix()}')")
    table = con.sql(SILVER_SQL).to_arrow_table()
    con.close()
    write_deltalake(str(paths.silver), table, mode="overwrite", schema_mode="overwrite")
    log.info("[silver] %s clean rows", f"{table.num_rows:,}")
    return table.num_rows


# --------------------------------------------------------------------------- Gold

GOLD_QUERIES: dict[str, str] = {
    # Congestion pricing: Manhattan pickups before vs after.
    "gold_congestion_impact": """
        SELECT
            congestion_era,
            COUNT(*)                          AS trips,
            ROUND(AVG(fare_amount), 2)        AS avg_fare,
            ROUND(AVG(tip_amount), 2)         AS avg_tip,
            ROUND(AVG(trip_distance), 2)      AS avg_distance,
            ROUND(AVG(trip_duration_min), 1)  AS avg_duration_min,
            ROUND(AVG(cbd_congestion_fee), 3) AS avg_congestion_fee
        FROM silver
        WHERE pickup_borough = 'Manhattan'
        GROUP BY congestion_era
        ORDER BY congestion_era
    """,
    "gold_demand_by_hour": """
        SELECT
            pickup_hour,
            congestion_era,
            COUNT(*)                   AS trips,
            ROUND(AVG(fare_amount), 2) AS avg_fare
        FROM silver
        GROUP BY pickup_hour, congestion_era
        ORDER BY pickup_hour, congestion_era
    """,
    "gold_revenue_by_borough": """
        SELECT
            pickup_borough,
            congestion_era,
            COUNT(*)                     AS trips,
            ROUND(SUM(total_amount), 2)  AS total_revenue,
            ROUND(AVG(total_amount), 2)  AS avg_revenue_per_trip
        FROM silver
        WHERE pickup_borough IS NOT NULL
        GROUP BY pickup_borough, congestion_era
        ORDER BY total_revenue DESC
    """,
    "gold_top_routes": """
        SELECT
            pickup_zone,
            dropoff_zone,
            COUNT(*)                   AS trips,
            ROUND(AVG(fare_amount), 2) AS avg_fare
        FROM silver
        WHERE pickup_zone IS NOT NULL AND dropoff_zone IS NOT NULL
        GROUP BY pickup_zone, dropoff_zone
        ORDER BY trips DESC
        LIMIT 20
    """,
}


def build_gold(paths: LakehousePaths) -> dict[str, int]:
    """Write every Gold table from Silver; return the row count of each."""
    paths.gold_dir.mkdir(parents=True, exist_ok=True)
    con = connect(silver=paths.silver)
    counts = {}
    for name, query in GOLD_QUERIES.items():
        table = con.sql(query).to_arrow_table()
        write_deltalake(str(paths.gold(name)), table, mode="overwrite", schema_mode="overwrite")
        counts[name] = table.num_rows
        log.info("[gold] %s: %s rows", name, f"{table.num_rows:,}")
    con.close()
    return counts


# --------------------------------------------------------------------------- quality measurements


def scalar(sql: str, **tables: Path) -> float | int | None:
    """Run a one-value query over the named Delta tables."""
    con = connect(**tables)
    value = con.execute(sql).fetchone()[0]
    con.close()
    return value


def count_rows(path: Path) -> int:
    return int(scalar("SELECT COUNT(*) FROM t", t=path))


def count_violations(path: Path, condition: str) -> int:
    """Rows of the Delta table at `path` that match `condition` (rows that should not exist)."""
    return int(scalar(f"SELECT COUNT(*) FROM t WHERE {condition}", t=path))


def columns(path: Path) -> list[str]:
    return [field.name for field in DeltaTable(str(path)).schema().fields]
