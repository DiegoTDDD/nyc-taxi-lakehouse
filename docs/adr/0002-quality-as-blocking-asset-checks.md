# 0002. Data quality as Dagster asset checks; the critical ones block the next layer

**Status:** Accepted. Before this there were two non-blocking checks on Silver.

## Context
Silver removes about 19% of Bronze as invalid. A bug in the cleaning rules, or a change in the source files, could let bad rows reach Gold, and with them the published numbers. The checks also need to show up where the pipeline is run and watched.

## Decision
Ten asset checks in `definitions.py`, each attached to the asset it measures:

| Layer | Check | Blocking |
|---|---|---|
| Bronze | not empty | yes |
| Bronze | 2025 files bring `cbd_congestion_fee` | warning |
| Silver | no non-positive fares or totals | yes |
| Silver | durations between 1 minute and 24 hours | yes |
| Silver | distance, passengers and timestamps within the cleaning rules | yes |
| Silver | no nulls in the columns Gold groups or sums by | yes |
| Silver | Silver ≤ Bronze, and at most 30% of rows removed | yes |
| Silver | at most 1% of pickup zones unmatched in the lookup | warning |
| Gold | trips in `gold_demand_by_hour` equal Silver's row count | no |
| Gold | the congestion comparison has both eras | no |

A blocking check that fails stops the run before the next asset is built. The 30% limit is a setting on the `Lakehouse` resource.

## Alternatives considered
- **Great Expectations or Soda.** Richer rule libraries and reports, but another tool and its configuration, for rules that each fit in one SQL condition.
- **Checks only in tests.** Tests prove the code on sample rows. Asset checks measure the real data on every run.

## Consequences
- Bad Silver cannot reach Gold. `tests/test_definitions.py` plants a negative fare and checks that the run fails and Gold is not written.
- Each check is one more query over Silver. On 18 million rows that takes seconds.
