"""Download NYC Yellow Taxi Parquet files from the TLC for the target months."""

from pathlib import Path
import requests

# Months chosen to straddle the January 2025 congestion pricing rollout:
# three months before (2024) and three months after (2025).
MONTHS = [
    "2024-10", "2024-11", "2024-12",
    "2025-01", "2025-02", "2025-03",
]

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# data/raw lives two levels up from this file (src/nyc_taxi_lakehouse/ -> project root)
RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


def download_month(month: str) -> None:
    """Download a single month's Yellow Taxi file, skipping if already present."""
    filename = f"yellow_tripdata_{month}.parquet"
    url = f"{BASE_URL}/{filename}"
    destination = RAW_DIR / filename

    if destination.exists():
        print(f"[skip] {filename} already downloaded")
        return

    print(f"[downloading] {filename} ...")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    with open(destination, "wb") as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            file.write(chunk)

    size_mb = destination.stat().st_size / (1024 * 1024)
    print(f"[done] {filename} ({size_mb:.1f} MB)")


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Saving files to: {RAW_DIR}")
    for month in MONTHS:
        download_month(month)
    print("\nAll downloads finished.")


if __name__ == "__main__":
    main()