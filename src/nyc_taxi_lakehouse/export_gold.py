"""Export the Gold Delta tables to Parquet files for the deployed dashboard.

The deployed dashboard reads these small Parquet files (no Delta reader needed),
which keeps the cloud deploy light.

Usage:
    python src/nyc_taxi_lakehouse/export_gold.py
"""

import logging

from deltalake import DeltaTable
from pipeline import GOLD_QUERIES, LakehousePaths

log = logging.getLogger(__name__)


def export(paths: LakehousePaths) -> None:
    export_dir = paths.data_dir / "gold_parquet"
    export_dir.mkdir(parents=True, exist_ok=True)
    for name in GOLD_QUERIES:
        target = export_dir / f"{name}.parquet"
        DeltaTable(str(paths.gold(name))).to_pandas().to_parquet(target, index=False)
        log.info("[export] %s -> %s", name, target)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    export(LakehousePaths())


if __name__ == "__main__":
    main()
