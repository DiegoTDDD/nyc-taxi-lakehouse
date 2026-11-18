"""Bronze layer: ingest the raw TLC Parquet files into a Delta table, faithfully.

Standalone alternative to materializing the `bronze_trips` asset in Dagster; both
call pipeline.py.

Usage:
    python src/nyc_taxi_lakehouse/bronze.py
"""

import logging

from pipeline import LakehousePaths, ingest_bronze


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ingest_bronze(LakehousePaths())


if __name__ == "__main__":
    main()
