"""NYC Taxi Lakehouse — Streamlit dashboard reading the Gold Delta tables.

The headline is the congestion pricing impact (Jan 2025): how Manhattan trips
changed before vs after the fee was introduced.
"""

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
GOLD_DIR = PROJECT_ROOT / "data" / "lakehouse" / "gold"

st.set_page_config(page_title="NYC Taxi Lakehouse", page_icon="🚕", layout="wide")


@st.cache_data
def load_gold(table_name: str) -> pd.DataFrame:
    """Read a Gold Delta table into a pandas DataFrame via DuckDB."""
    table_path = GOLD_DIR / table_name
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    df = con.execute(f"SELECT * FROM delta_scan('{table_path.as_posix()}')").df()
    con.close()
    return df


st.title("🚕 NYC Yellow Taxi — Lakehouse Analytics")
st.caption(
    "A medallion-architecture lakehouse (Bronze → Silver → Gold) over 22M+ trips, "
    "built with Delta Lake, DuckDB, and Dagster. Headline: the impact of NYC's "
    "January 2025 congestion pricing."
)

if not GOLD_DIR.exists():
    st.error(
        "Gold tables not found. Run the pipeline first "
        "(via Dagster 'Materialize all' or the gold.py script)."
    )
    st.stop()

# ---- Section 1: Congestion pricing impact (headline) ----
st.header("Congestion Pricing Impact — Manhattan")

impact = load_gold("gold_congestion_impact")
before = impact[impact["congestion_era"] == "before"].iloc[0]
after = impact[impact["congestion_era"] == "after"].iloc[0]


def pct_change(old: float, new: float) -> str:
    return f"{(new - old) / old * 100:+.1f}%"


col1, col2, col3, col4 = st.columns(4)
col1.metric("Trips", f"{after['trips']:,.0f}", pct_change(before["trips"], after["trips"]))
col2.metric("Avg fare", f"${after['avg_fare']:.2f}", pct_change(before["avg_fare"], after["avg_fare"]))
col3.metric(
    "Avg duration (min)",
    f"{after['avg_duration_min']:.1f}",
    pct_change(before["avg_duration_min"], after["avg_duration_min"]),
)
col4.metric(
    "Avg congestion fee",
    f"${after['avg_congestion_fee']:.3f}",
    "new in 2025",
)

st.info(
    "After the January 2025 congestion fee, Manhattan trips dropped in volume and "
    "got shorter and faster — average trip duration fell, consistent with the policy's "
    "goal of reducing congestion. (Note: the comparison spans different seasons, so "
    "part of the change may be seasonal.)"
)

# ---- Section 2: Demand by hour ----
st.header("Demand by Hour of Day")
demand = load_gold("gold_demand_by_hour")
fig_demand = px.line(
    demand,
    x="pickup_hour",
    y="trips",
    color="congestion_era",
    markers=True,
    labels={"pickup_hour": "Hour of day", "trips": "Trips", "congestion_era": "Era"},
)
st.plotly_chart(fig_demand, use_container_width=True)

# ---- Section 3: Revenue by borough ----
st.header("Revenue by Pickup Borough")
revenue = load_gold("gold_revenue_by_borough")
fig_rev = px.bar(
    revenue,
    x="pickup_borough",
    y="total_revenue",
    color="congestion_era",
    barmode="group",
    labels={
        "pickup_borough": "Borough",
        "total_revenue": "Total revenue ($)",
        "congestion_era": "Era",
    },
)
st.plotly_chart(fig_rev, use_container_width=True)

# ---- Section 4: Top routes ----
st.header("Top 20 Routes")
routes = load_gold("gold_top_routes")
routes_display = routes.rename(
    columns={
        "pickup_zone": "From",
        "dropoff_zone": "To",
        "trips": "Trips",
        "avg_fare": "Avg fare ($)",
    }
)
st.dataframe(routes_display, use_container_width=True, hide_index=True)

st.caption("Data source: NYC TLC Trip Record Data. Pipeline orchestrated with Dagster.")
