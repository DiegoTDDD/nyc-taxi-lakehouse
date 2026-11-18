# 0005. The deployed dashboard reads a committed Parquet export of Gold

**Status:** Accepted

## Context
Streamlit Community Cloud cannot download 360 MB of TLC data or run the pipeline. Gold is small: four tables of at most a few dozen rows.

## Decision
`export_gold.py` writes each Gold table to `data/gold_parquet/`, which is committed. The dashboard reads those files.

## Alternatives considered
- **Run the pipeline on deploy.** Too slow and too large for the free host.
- **A hosted database.** Costs money and needs credentials.

## Consequences
- The public dashboard always loads, and quickly.
- It shows the last exported run. The monthly how-to check rebuilds Gold from the real data and fails if it differs from the committed files.
