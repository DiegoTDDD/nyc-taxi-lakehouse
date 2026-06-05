"""Export the Gold Delta tables to Parquet files for the deployed dashboard.

The deployed dashboard reads these small Parquet files (no Delta extension needed),
which keeps the cloud deploy robust and lightweight.
"""

from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_ROOT / "data" / "lakehouse" / "gold"
EXPORT_DIR = PROJECT_ROOT / "data" / "gold_parquet"

TABLES = [
    "gold_congestion_impact",
    "gold_demand_by_hour",
    "gold_revenue_by_borough",
    "gold_top_routes",
]


def main() -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    for table in TABLES:
        src = (GOLD_DIR / table).as_posix()
        dst = (EXPORT_DIR / f"{table}.parquet").as_posix()
        con.execute(f"COPY (SELECT * FROM delta_scan('{src}')) TO '{dst}' (FORMAT PARQUET)")
        print(f"[export] {table} -> {dst}")
    con.close()
    print("\nExport complete.")


if __name__ == "__main__":
    main()