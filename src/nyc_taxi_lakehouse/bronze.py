"""Bronze layer: ingest raw TLC Parquet files into a Delta table, faithfully.

No cleaning happens here. We only add ingestion metadata and let the Delta
table evolve its schema as the 2025 files introduce the cbd_congestion_fee column.
"""

from datetime import datetime, timezone
from pathlib import Path

import duckdb
from deltalake import write_deltalake

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_PATH = PROJECT_ROOT / "data" / "lakehouse" / "bronze"


def ingest_file(parquet_path: Path, is_first_write: bool) -> int:
    """Read one raw Parquet file, add metadata, write to the Bronze Delta table."""
    con = duckdb.connect()

    # Read the full file into an Arrow table, adding ingestion metadata columns.
    ingested_at = datetime.now(timezone.utc)
    source_file = parquet_path.name

    arrow_table = con.execute(
        f"""
        SELECT
            *,
            TIMESTAMP '{ingested_at:%Y-%m-%d %H:%M:%S}' AS _ingested_at,
            '{source_file}' AS _source_file
        FROM '{parquet_path}'
        """
    ).to_arrow_table()
    con.close()

    write_deltalake(
        str(BRONZE_PATH),
        arrow_table,
        mode="overwrite" if is_first_write else "append",
        schema_mode="overwrite" if is_first_write else "merge",
    )
    return arrow_table.num_rows


def main() -> None:
    BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)

    raw_files = sorted(RAW_DIR.glob("yellow_tripdata_*.parquet"))
    if not raw_files:
        print("No raw files found. Run download_data.py first.")
        return

    total_rows = 0
    for index, parquet_path in enumerate(raw_files):
        print(f"[bronze] ingesting {parquet_path.name} ...")
        rows = ingest_file(parquet_path, is_first_write=(index == 0))
        total_rows += rows
        print(f"[bronze] {parquet_path.name}: {rows:,} rows")

    print(f"\nBronze layer complete: {total_rows:,} total rows written to {BRONZE_PATH}")


if __name__ == "__main__":
    main()