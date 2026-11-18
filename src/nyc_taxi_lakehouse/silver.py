"""Silver layer: clean, validate and enrich the Bronze data.

Standalone alternative to materializing the `silver_trips` asset in Dagster; both
call pipeline.py.

Usage:
    python src/nyc_taxi_lakehouse/silver.py
"""

import logging

from pipeline import LakehousePaths, build_silver


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    build_silver(LakehousePaths())


if __name__ == "__main__":
    main()
