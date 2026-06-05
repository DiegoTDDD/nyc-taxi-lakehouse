"""Sanity checks on the Silver Delta table."""

from pathlib import Path
import duckdb

SILVER_PATH = Path(__file__).resolve().parents[2] / "data" / "lakehouse" / "silver"

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")
silver = f"delta_scan('{SILVER_PATH.as_posix()}')"

print("Trips by congestion era:")
for era, n in con.execute(
    f"SELECT congestion_era, COUNT(*) FROM {silver} GROUP BY congestion_era ORDER BY congestion_era"
).fetchall():
    print(f"  {era}: {n:,}")

print("\nTop 5 pickup boroughs:")
for borough, n in con.execute(
    f"SELECT pickup_borough, COUNT(*) AS n FROM {silver} GROUP BY pickup_borough ORDER BY n DESC LIMIT 5"
).fetchall():
    print(f"  {borough}: {n:,}")

print("\nAverage cbd_congestion_fee by era (should be ~0 before, >0 after):")
for era, avg_fee in con.execute(
    f"SELECT congestion_era, ROUND(AVG(cbd_congestion_fee), 3) FROM {silver} GROUP BY congestion_era ORDER BY congestion_era"
).fetchall():
    print(f"  {era}: ${avg_fee}")

print("\nAverage fare and trip distance overall:")
avg_fare, avg_dist = con.execute(
    f"SELECT ROUND(AVG(fare_amount), 2), ROUND(AVG(trip_distance), 2) FROM {silver}"
).fetchone()
print(f"  avg fare: ${avg_fare}  |  avg distance: {avg_dist} miles")

con.close()