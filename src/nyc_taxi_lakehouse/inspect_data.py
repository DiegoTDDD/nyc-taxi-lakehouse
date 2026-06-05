"""Quick inspection of the raw Parquet files: schema and a few rows."""

from pathlib import Path
import duckdb

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"

# Compare the oldest (2024, pre-congestion-fee) with the newest (2025, post).
files_to_inspect = ["yellow_tripdata_2024-10.parquet", "yellow_tripdata_2025-01.parquet"]

con = duckdb.connect()

for filename in files_to_inspect:
    path = RAW_DIR / filename
    print("=" * 70)
    print(f"FILE: {filename}")
    print("=" * 70)

    # Row count
    count = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
    print(f"Rows: {count:,}")

    # Column names and types
    print("\nColumns:")
    schema = con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()
    for column_name, column_type, *_ in schema:
        print(f"  - {column_name}: {column_type}")
    print()

con.close()