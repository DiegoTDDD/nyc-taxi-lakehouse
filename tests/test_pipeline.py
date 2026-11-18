"""Bronze, Silver and Gold on the tiny lakehouse from conftest.py."""

import pandas as pd
import pytest
from deltalake import DeltaTable
from pipeline import build_gold, build_silver, columns, count_rows, ingest_bronze


def read(path) -> pd.DataFrame:
    return DeltaTable(str(path)).to_pandas()


def test_bronze_keeps_every_row_and_evolves_the_schema(paths):
    assert ingest_bronze(paths) == 10
    bronze = read(paths.bronze)
    assert len(bronze) == 10  # nothing is cleaned in Bronze
    assert {"_ingested_at", "_source_file", "cbd_congestion_fee"} <= set(bronze.columns)
    by_file = bronze.groupby("_source_file")["cbd_congestion_fee"]
    assert by_file.apply(lambda s: s.isna().all())["yellow_tripdata_2024-12.parquet"]
    assert (by_file.max()["yellow_tripdata_2025-01.parquet"]) == 0.75


def test_bronze_needs_raw_files(tmp_path):
    from pipeline import LakehousePaths

    with pytest.raises(FileNotFoundError):
        ingest_bronze(LakehousePaths(tmp_path))


def test_silver_drops_invalid_trips_and_enriches_the_rest(paths):
    ingest_bronze(paths)
    assert build_silver(paths) == 5
    silver = read(paths.silver).sort_values(["pickup_datetime", "fare_amount"])
    assert silver["fare_amount"].gt(0).all()
    assert silver["trip_distance"].lt(100).all()
    assert silver["trip_duration_min"].between(1, 1440).all()
    assert silver["congestion_era"].tolist() == ["before"] * 3 + ["after"] * 2
    assert silver["cbd_congestion_fee"].tolist() == [0.0, 0.0, 0.0, 0.75, 0.75]  # 2024 nulls -> 0
    before = silver[silver["congestion_era"] == "before"]
    assert sorted(before["pickup_borough"]) == ["Manhattan", "Manhattan", "Queens"]
    unknown = silver[silver["pickup_location_id"] == 99]
    assert len(unknown) == 1 and unknown["pickup_borough"].isna().all()  # kept, zone unresolved
    assert set(silver["pickup_hour"]) == {8, 18}
    expected = {"vendor_id", "pickup_zone", "dropoff_zone", "pickup_day_of_week"}
    assert expected <= set(columns(paths.silver))


def test_gold_tables_aggregate_silver(paths):
    ingest_bronze(paths)
    build_silver(paths)
    counts = build_gold(paths)
    assert set(counts) == {"gold_congestion_impact", "gold_demand_by_hour",
                           "gold_revenue_by_borough", "gold_top_routes"}

    impact = read(paths.gold("gold_congestion_impact")).set_index("congestion_era")
    assert impact.loc["before", "trips"] == 2  # Manhattan pickups only
    assert impact.loc["before", "avg_fare"] == 20.0  # (15 + 25) / 2
    assert impact.loc["after", "trips"] == 1
    assert impact.loc["after", "avg_congestion_fee"] == 0.75
    assert impact.loc["after", "avg_duration_min"] == 10.0

    demand = read(paths.gold("gold_demand_by_hour"))
    assert demand["trips"].sum() == count_rows(paths.silver)

    revenue = read(paths.gold("gold_revenue_by_borough"))
    assert revenue["pickup_borough"].notna().all()
    manhattan_before = revenue.query("pickup_borough == 'Manhattan' and congestion_era == 'before'")
    assert manhattan_before["total_revenue"].item() == 50.0

    routes = read(paths.gold("gold_top_routes"))
    assert routes.iloc[0][["pickup_zone", "dropoff_zone", "trips"]].tolist() == [
        "Midtown", "Upper East Side", 3
    ]
