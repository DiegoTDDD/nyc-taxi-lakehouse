"""Dagster definitions: the medallion pipeline as a graph of assets with data-quality checks.

bronze_trips -> silver_trips -> gold_tables. The assets call pipeline.py; the
checks measure each layer after it is written. Checks marked `blocking` stop the
run before a bad layer feeds the next one (ADR 0002).

Run:
    dagster dev -f src/nyc_taxi_lakehouse/definitions.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    ConfigurableResource,
    Definitions,
    MaterializeResult,
    asset,
    asset_check,
)

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline import (  # noqa: E402
    MAX_REMOVED_SHARE,
    PROJECT_ROOT,
    LakehousePaths,
    build_gold,
    build_silver,
    columns,
    count_rows,
    count_violations,
    ingest_bronze,
    scalar,
)


class Lakehouse(ConfigurableResource):
    """The data folder the pipeline reads and writes (a temporary one in tests)."""

    data_dir: str = str(PROJECT_ROOT / "data")
    # Share of Bronze that Silver may drop as invalid before the run stops (real data: ~19%).
    max_removed_share: float = MAX_REMOVED_SHARE

    @property
    def paths(self) -> LakehousePaths:
        return LakehousePaths(Path(self.data_dir))


def _result(bad: int, **metadata: object) -> AssetCheckResult:
    return AssetCheckResult(passed=bad == 0, metadata={"bad_rows": bad, **metadata})


# --------------------------------------------------------------------------- assets


@asset
def bronze_trips(lakehouse: Lakehouse) -> MaterializeResult:
    """Raw TLC Parquet files ingested faithfully into a Delta table."""
    return MaterializeResult(metadata={"rows": ingest_bronze(lakehouse.paths)})


@asset(deps=[bronze_trips])
def silver_trips(lakehouse: Lakehouse) -> MaterializeResult:
    """Cleaned, validated and enriched trips with zone names and congestion era."""
    return MaterializeResult(metadata={"rows": build_silver(lakehouse.paths)})


@asset(deps=[silver_trips])
def gold_tables(lakehouse: Lakehouse) -> MaterializeResult:
    """Business-ready aggregate tables built from Silver."""
    counts = build_gold(lakehouse.paths)
    return MaterializeResult(metadata={"gold_rows_total": sum(counts.values()), **counts})


# --------------------------------------------------------------------------- Bronze checks


@asset_check(asset=bronze_trips, blocking=True)
def bronze_not_empty(lakehouse: Lakehouse) -> AssetCheckResult:
    """Bronze must hold rows from at least one source file."""
    rows = count_rows(lakehouse.paths.bronze)
    files = scalar("SELECT COUNT(DISTINCT _source_file) FROM b", b=lakehouse.paths.bronze)
    return AssetCheckResult(passed=rows > 0, metadata={"rows": rows, "source_files": files})


@asset_check(asset=bronze_trips)
def bronze_schema_evolved(lakehouse: Lakehouse) -> AssetCheckResult:
    """When 2025 files are loaded, Bronze must carry their cbd_congestion_fee column."""
    paths = lakehouse.paths
    has_2025 = any(paths.raw_dir.glob("yellow_tripdata_2025-*.parquet"))
    has_column = "cbd_congestion_fee" in columns(paths.bronze)
    return AssetCheckResult(
        passed=has_column or not has_2025,
        severity=AssetCheckSeverity.WARN,
        metadata={"has_2025_files": has_2025, "has_cbd_congestion_fee": has_column},
    )


# --------------------------------------------------------------------------- Silver checks


@asset_check(asset=silver_trips, blocking=True)
def silver_no_negative_fares(lakehouse: Lakehouse) -> AssetCheckResult:
    """Silver must contain no non-positive fares or totals."""
    return _result(
        count_violations(lakehouse.paths.silver, "fare_amount <= 0 OR total_amount <= 0")
    )


@asset_check(asset=silver_trips, blocking=True)
def silver_valid_durations(lakehouse: Lakehouse) -> AssetCheckResult:
    """Trip durations must be within 1 minute and 24 hours."""
    return _result(count_violations(
        lakehouse.paths.silver, "trip_duration_min < 1 OR trip_duration_min > 1440"
    ))


@asset_check(asset=silver_trips, blocking=True)
def silver_values_in_range(lakehouse: Lakehouse) -> AssetCheckResult:
    """Distance, passengers and timestamps must respect the cleaning rules."""
    return _result(count_violations(
        lakehouse.paths.silver,
        "trip_distance <= 0 OR trip_distance >= 100 OR passenger_count NOT BETWEEN 1 AND 6 "
        "OR dropoff_datetime <= pickup_datetime",
    ))


@asset_check(asset=silver_trips, blocking=True)
def silver_no_null_keys(lakehouse: Lakehouse) -> AssetCheckResult:
    """The columns every Gold table groups or sums by must never be null."""
    keys = ["pickup_datetime", "dropoff_datetime", "fare_amount", "total_amount",
            "pickup_hour", "congestion_era", "cbd_congestion_fee"]
    return _result(count_violations(
        lakehouse.paths.silver, " OR ".join(f"{k} IS NULL" for k in keys)
    ))


@asset_check(asset=silver_trips, blocking=True)
def silver_reconciles_with_bronze(lakehouse: Lakehouse) -> AssetCheckResult:
    """Silver can only lose rows, and at most `max_removed_share` of Bronze."""
    bronze = count_rows(lakehouse.paths.bronze)
    silver = count_rows(lakehouse.paths.silver)
    removed = (bronze - silver) / bronze if bronze else 0.0
    return AssetCheckResult(
        passed=silver <= bronze and removed <= lakehouse.max_removed_share,
        metadata={"bronze_rows": bronze, "silver_rows": silver, "removed_share": round(removed, 4),
                  "max_removed_share": lakehouse.max_removed_share},
    )


@asset_check(asset=silver_trips)
def silver_zones_resolved(lakehouse: Lakehouse) -> AssetCheckResult:
    """Almost every pickup zone should match the TLC zone lookup (warning only)."""
    total = count_rows(lakehouse.paths.silver)
    missing = count_violations(lakehouse.paths.silver, "pickup_borough IS NULL")
    share = missing / total if total else 0.0
    return AssetCheckResult(
        passed=share <= 0.01,
        severity=AssetCheckSeverity.WARN,
        metadata={"unmatched_pickup_zones": missing, "unmatched_share": round(share, 4)},
    )


# --------------------------------------------------------------------------- Gold checks


@asset_check(asset=gold_tables)
def gold_demand_reconciles_with_silver(lakehouse: Lakehouse) -> AssetCheckResult:
    """Trips in gold_demand_by_hour must add up to exactly the Silver row count."""
    paths = lakehouse.paths
    gold = int(scalar("SELECT SUM(trips) FROM g", g=paths.gold("gold_demand_by_hour")))
    silver = count_rows(paths.silver)
    return AssetCheckResult(passed=gold == silver,
                            metadata={"gold_trips": gold, "silver_rows": silver})


@asset_check(asset=gold_tables)
def gold_congestion_impact_has_both_eras(lakehouse: Lakehouse) -> AssetCheckResult:
    """The headline comparison needs Manhattan trips before and after the fee."""
    eras = scalar("SELECT COUNT(DISTINCT congestion_era) FROM g",
                  g=lakehouse.paths.gold("gold_congestion_impact"))
    return AssetCheckResult(passed=eras == 2, metadata={"eras": eras})


defs = Definitions(
    assets=[bronze_trips, silver_trips, gold_tables],
    asset_checks=[
        bronze_not_empty,
        bronze_schema_evolved,
        silver_no_negative_fares,
        silver_valid_durations,
        silver_values_in_range,
        silver_no_null_keys,
        silver_reconciles_with_bronze,
        silver_zones_resolved,
        gold_demand_reconciles_with_silver,
        gold_congestion_impact_has_both_eras,
    ],
    resources={"lakehouse": Lakehouse()},
)
