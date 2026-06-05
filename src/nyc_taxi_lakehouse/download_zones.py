"""Download the TLC taxi zone lookup table (maps LocationID to borough/zone)."""

from pathlib import Path
import requests

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    destination = RAW_DIR / "taxi_zone_lookup.csv"

    print(f"[downloading] taxi_zone_lookup.csv ...")
    response = requests.get(URL, timeout=60)
    response.raise_for_status()
    destination.write_bytes(response.content)

    size_kb = destination.stat().st_size / 1024
    print(f"[done] taxi_zone_lookup.csv ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()