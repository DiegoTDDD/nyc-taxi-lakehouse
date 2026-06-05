"""Gold layer: business-ready aggregate tables built from Silver.

Each table answers a specific business question and is small enough to ship
to the dashboard. The congestion pricing comparison is the headline.
"""

from pathlib import Path

import duckdb
from deltalake import write_deltalake

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_PATH = PROJECT_ROOT / "data" / "lakehouse" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "lakehouse" / "gold"


def write_gold(con, name: str, query: str) -> int:
    arrow_table = con.execute(query).to_arrow_table()
    write_deltalake(str(GOLD_DIR / name), arrow_table, mode="overwrite", schema_mode="overwrite")
    print(f"[gold] {name}: {arrow_table.num_rows:,} rows")
    return arrow_table.num_rows


def main() -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    silver = f"delta_scan('{SILVER_PATH.as_posix()}')"

    # 1. Congestion pricing impact: before vs after, Manhattan only.
    write_gold(con, "gold_congestion_impact", f"""
        SELECT
            congestion_era,
            COUNT(*)                          AS trips,
            ROUND(AVG(fare_amount), 2)        AS avg_fare,
            ROUND(AVG(tip_amount), 2)         AS avg_tip,
            ROUND(AVG(trip_distance), 2)      AS avg_distance,
            ROUND(AVG(trip_duration_min), 1)  AS avg_duration_min,
            ROUND(AVG(cbd_congestion_fee), 3) AS avg_congestion_fee
        FROM {silver}
        WHERE pickup_borough = 'Manhattan'
        GROUP BY congestion_era
        ORDER BY congestion_era
    """)

    # 2. Demand by hour of day and era.
    write_gold(con, "gold_demand_by_hour", f"""
        SELECT
            pickup_hour,
            congestion_era,
            COUNT(*)                   AS trips,
            ROUND(AVG(fare_amount), 2) AS avg_fare
        FROM {silver}
        GROUP BY pickup_hour, congestion_era
        ORDER BY pickup_hour, congestion_era
    """)

    # 3. Revenue by borough and era.
    write_gold(con, "gold_revenue_by_borough", f"""
        SELECT
            pickup_borough,
            congestion_era,
            COUNT(*)                     AS trips,
            ROUND(SUM(total_amount), 2)  AS total_revenue,
            ROUND(AVG(total_amount), 2)  AS avg_revenue_per_trip
        FROM {silver}
        WHERE pickup_borough IS NOT NULL
        GROUP BY pickup_borough, congestion_era
        ORDER BY total_revenue DESC
    """)

    # 4. Top 20 routes (pickup zone -> dropoff zone).
    write_gold(con, "gold_top_routes", f"""
        SELECT
            pickup_zone,
            dropoff_zone,
            COUNT(*)                   AS trips,
            ROUND(AVG(fare_amount), 2) AS avg_fare
        FROM {silver}
        WHERE pickup_zone IS NOT NULL AND dropoff_zone IS NOT NULL
        GROUP BY pickup_zone, dropoff_zone
        ORDER BY trips DESC
        LIMIT 20
    """)

    con.close()
    print(f"\nGold layer complete. Tables written to {GOLD_DIR}")


if __name__ == "__main__":
    main()