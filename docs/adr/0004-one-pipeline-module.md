# 0004. One pipeline module; Dagster assets and scripts are thin wrappers

**Status:** Accepted. Before this, the Silver and Gold SQL existed twice: in `silver.py`/`gold.py` and again inside `definitions.py`.

## Context
Two copies of the same SQL drift apart. A fix in one copy would leave the other running the old logic, depending on whether the pipeline was run from Dagster or from the scripts. The paths were also hard-coded, so nothing could run on test data.

## Decision
`pipeline.py` holds the Bronze ingestion, the Silver SQL and its cleaning rules, the Gold queries, and helpers for quality checks. `LakehousePaths` points everything at one data folder. The Dagster assets get that folder from a `Lakehouse` resource, and `bronze.py`, `silver.py` and `gold.py` call the same functions.

## Consequences
- One place to change a rule, and the tests exercise exactly the code Dagster runs.
- Tests point `Lakehouse(data_dir=...)` at a temporary folder with ten hand-made trips.
