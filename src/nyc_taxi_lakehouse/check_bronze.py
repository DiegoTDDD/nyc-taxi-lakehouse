"""Sanity checks on the Bronze Delta table."""

from pathlib import Path
import duckdb

BRONZE_PATH = Path(__file__).resolve().parents[2] / "data" / "lakehouse" / "bronze"

con = duckdb.connect()
# delta_scan reads a Delta table directly with DuckDB.
con.execute("INSTALL delta; LOAD delta;")

bronze = f"delta_scan('{BRONZE_PATH.as_posix()}')"

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