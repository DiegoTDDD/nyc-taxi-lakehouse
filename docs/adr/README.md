# Architecture decision records

| # | Decision | Status |
|---|---|---|
| [0001](0001-delta-lake-and-duckdb.md) | Delta Lake files queried with DuckDB, not Spark | Accepted |
| [0002](0002-quality-as-blocking-asset-checks.md) | Data quality as Dagster asset checks; the critical ones block the next layer | Accepted |
| [0003](0003-read-delta-without-the-duckdb-extension.md) | Read Delta with `deltalake` and hand Arrow datasets to DuckDB | Accepted, replaces DuckDB's `delta` extension |
| [0004](0004-one-pipeline-module.md) | One pipeline module; Dagster assets and scripts are thin wrappers | Accepted, replaces two copies of the SQL |
| [0005](0005-dashboard-reads-a-parquet-snapshot.md) | The deployed dashboard reads a committed Parquet export of Gold | Accepted |
