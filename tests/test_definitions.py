"""The Dagster graph: materialize everything on the tiny lakehouse, then break Silver on purpose."""

import definitions as d
import pandas as pd
from dagster import materialize
from deltalake import write_deltalake

ASSETS = [d.bronze_trips, d.silver_trips, d.gold_tables]
CHECKS = list(d.defs.asset_checks)


def run_all(paths, **kwargs):
    """Same as "Materialize all" in the UI: every asset and every check."""
    return materialize(
        [*ASSETS, *CHECKS],
        # The sample drops 5 of 10 rows on purpose; real TLC data drops ~19%.
        resources={"lakehouse": d.Lakehouse(data_dir=str(paths.data_dir), max_removed_share=0.6)},
        **kwargs,
    )


def check_results(result):
    """check name -> passed, for every asset check that ran."""
    return {e.check_name: e.passed for e in result.get_asset_check_evaluations()}


def test_definitions_load():
    assert d.defs.get_repository_def().get_all_jobs() is not None


def test_materialize_all_runs_every_check(paths):
    # Real TLC data has almost every zone resolved; this sample has 1 of 5 unknown on purpose.
    result = run_all(paths)
    assert result.success
    checks = check_results(result)
    assert len(checks) == 10
    failed = {name for name, passed in checks.items() if not passed}
    assert failed == {"silver_zones_resolved"}  # a warning, not blocking


def test_blocking_check_stops_bad_silver_reaching_gold(paths, monkeypatch):
    # Make Silver keep a negative fare: the blocking check must fail before Gold is built.
    import pipeline

    real = pipeline.build_silver

    def leaky_silver(p):
        rows = real(p)
        bad = pd.DataFrame(d_row(p))
        write_deltalake(str(p.silver), bad, mode="append", schema_mode="merge")
        return rows + 1

    monkeypatch.setattr(d, "build_silver", leaky_silver)
    result = run_all(paths, raise_on_error=False)
    assert not result.success
    assert check_results(result)["silver_no_negative_fares"] is False
    assert not paths.gold("gold_demand_by_hour").exists()


def d_row(p):
    from deltalake import DeltaTable

    one = DeltaTable(str(p.silver)).to_pandas().head(1).copy()
    one["fare_amount"] = -1.0
    return one
