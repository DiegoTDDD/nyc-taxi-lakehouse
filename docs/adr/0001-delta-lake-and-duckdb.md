# 0001. Delta Lake files queried with DuckDB, not Spark

**Status:** Accepted

## Context
Six months of NYC Yellow Taxi data is 22.3 million rows, about 360 MB of Parquet. The project must run on a laptop and on free CI runners. It also needs a table format that handles the schema change in the 2025 files (the new `cbd_congestion_fee` column).

## Decision
Store every layer as a Delta Lake table, written with `deltalake` (delta-rs), and run all transformations as SQL in DuckDB, in process.

## Alternatives considered
- **Spark with Delta.** The standard at cluster scale, but it needs a JVM and has slow start-up and heavy local setup, with no benefit at 22 million rows.
- **Plain Parquet folders.** No transaction log, no schema evolution on append and no safe overwrite.
- **A warehouse (Postgres, BigQuery).** A service to run, or a bill.

## Consequences
- The full pipeline runs in a few minutes on one machine. The monthly how-to check runs it on the real data in CI.
- Delta's merge mode adds `cbd_congestion_fee` to Bronze when the 2025 files arrive. 2024 rows hold null, and Silver turns that into 0.
- Scaling past one machine would mean moving the SQL to Spark or a warehouse. The SQL itself is standard and would mostly carry over.
