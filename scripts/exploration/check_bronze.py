"""Sanity checks on the Bronze Delta table."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "nyc_taxi_lakehouse"))
from pipeline import connect  # noqa: E402

BRONZE_PATH = Path(__file__).resolve().parents[2] / "data" / "lakehouse" / "bronze"

con = connect(t=BRONZE_PATH)

bronze = "t"

print("Total rows in Bronze:")
total = con.execute(f"SELECT COUNT(*) FROM {bronze}").fetchone()[0]
print(f"  {total:,}\n")

print("Rows per source file:")
rows = con.execute(
    f"""
    SELECT _source_file, COUNT(*) AS n
    FROM {bronze}
    GROUP BY _source_file
    ORDER BY _source_file
    """
).fetchall()
for source_file, n in rows:
    print(f"  {source_file}: {n:,}")

print("\ncbd_congestion_fee: how many rows have a value vs null, per source file:")
fee = con.execute(
    f"""
    SELECT
        _source_file,
        COUNT(cbd_congestion_fee) AS has_value,
        COUNT(*) - COUNT(cbd_congestion_fee) AS is_null
    FROM {bronze}
    GROUP BY _source_file
    ORDER BY _source_file
    """
).fetchall()
for source_file, has_value, is_null in fee:
    print(f"  {source_file}: value={has_value:,}  null={is_null:,}")

con.close()