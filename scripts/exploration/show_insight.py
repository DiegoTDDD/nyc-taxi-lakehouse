"""Print the headline insight: congestion pricing impact in Manhattan."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "nyc_taxi_lakehouse"))
from pipeline import connect  # noqa: E402

GOLD_DIR = Path(__file__).resolve().parents[2] / "data" / "lakehouse" / "gold"

con = connect(t=(GOLD_DIR / 'gold_congestion_impact'))
impact = "t"

print("=" * 60)
print("CONGESTION PRICING IMPACT — Manhattan pickups")
print("=" * 60)

rows = con.execute(f"SELECT * FROM {impact} ORDER BY congestion_era").fetchall()
cols = [d[0] for d in con.description]

for row in rows:
    record = dict(zip(cols, row, strict=True))
    print(f"\n{record['congestion_era'].upper()}:")
    print(f"  Trips:            {record['trips']:,}")
    print(f"  Avg fare:         ${record['avg_fare']}")
    print(f"  Avg tip:          ${record['avg_tip']}")
    print(f"  Avg distance:     {record['avg_distance']} miles")
    print(f"  Avg duration:     {record['avg_duration_min']} min")
    print(f"  Avg congest. fee: ${record['avg_congestion_fee']}")

con.close()