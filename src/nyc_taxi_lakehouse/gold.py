"""Gold layer: business-ready aggregate tables built from Silver.

Standalone alternative to materializing the `gold_tables` asset in Dagster; both
call pipeline.py.

Usage:
    python src/nyc_taxi_lakehouse/gold.py
"""

import logging

from pipeline import LakehousePaths, build_gold


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    build_gold(LakehousePaths())


if __name__ == "__main__":
    main()
