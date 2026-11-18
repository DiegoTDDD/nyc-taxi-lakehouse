# 0003. Read Delta with `deltalake` and hand Arrow datasets to DuckDB

**Status:** Accepted. Replaces `INSTALL delta; LOAD delta;` with `delta_scan(...)`.

## Context
DuckDB's `delta` extension is downloaded from extensions.duckdb.org on first use. That fails offline, behind restrictive proxies, and whenever a new DuckDB release has not yet published its extension build. The pipeline already depends on `deltalake`, which reads Delta tables natively.

## Decision
`pipeline.connect()` opens each Delta table with `deltalake.DeltaTable(...).to_pyarrow_dataset()` and registers it in DuckDB under a name (`bronze`, `silver`). The SQL queries those names. Filters and column selection are still pushed down to the Arrow dataset.

## Alternatives considered
- **Keep the extension and pre-install it in CI.** It still breaks on any machine that cannot download it.
- **Load whole tables into pandas.** Copies 18 million rows into memory for no gain.

## Consequences
- No network access is needed after `pip install`. The unit tests run in a sandbox without internet.
- The SQL did not change. The monthly how-to check confirms that the real row counts (22,346,537 in Bronze and 18,165,564 in Silver) and the exported Gold tables are unchanged.
