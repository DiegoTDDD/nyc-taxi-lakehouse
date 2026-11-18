"""Download the TLC taxi zone lookup table (maps LocationID to borough/zone)."""

import logging
from pathlib import Path

import requests

log = logging.getLogger(__name__)

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    destination = RAW_DIR / "taxi_zone_lookup.csv"

    log.info("[downloading] taxi_zone_lookup.csv ...")
    response = requests.get(URL, timeout=60)
    response.raise_for_status()
    destination.write_bytes(response.content)

    size_kb = destination.stat().st_size / 1024
    log.info(f"[done] taxi_zone_lookup.csv ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()