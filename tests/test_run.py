from pathlib import Path

import pandas as pd

from pipeline import run


def test_should_skip_db_load_true(monkeypatch):
    monkeypatch.setenv("SKIP_DB_LOAD", "true")
    assert run._should_skip_db_load() is True


def test_should_skip_db_load_false(monkeypatch):
    monkeypatch.setenv("SKIP_DB_LOAD", "false")
    assert run._should_skip_db_load() is False


def test_run_pipeline_generates_gold_and_skips_db(monkeypatch):
    silver_df = pd.DataFrame(
        [
            {
                "base_currency": "USD",
                "target_currency": "BRL",
                "rate": 5.0,
                "reference_date": "2026-03-06",
            }
        ]
    )
    gold_df = pd.DataFrame(
        [
            {
                "base_currency": "USD",
                "target_currency": "BRL",
                "reference_date": "2026-03-06",
                "rate": 5.0,
                "previous_rate": None,
                "rate_change": None,
                "rate_change_pct": None,
                "trend_direction": "NEW",
                "rate_rank": 1,
                "currencies_in_snapshot": 1,
                "snapshot_generated_at": "2026-03-06T00:00:00Z",
            }
        ]
    )

    monkeypatch.setenv("SKIP_DB_LOAD", "true")
    monkeypatch.setattr(run, "fetch_exchange_rates", lambda: {"base": "USD", "rates": {"BRL": 5.0}})
    monkeypatch.setattr(run, "save_raw_data", lambda _payload: Path("data/raw/sample.json"))
    monkeypatch.setattr(run, "transform_latest_file", lambda: silver_df)
    monkeypatch.setattr(run, "save_processed_data", lambda _df: Path("data/processed/sample.parquet"))
    monkeypatch.setattr(
        run, "build_and_save_gold_data", lambda: (gold_df, Path("data/gold/sample.parquet"))
    )

    called = {"migrations": 0, "create_run": 0, "load": 0, "finalize": 0}
    monkeypatch.setattr(run, "apply_migrations", lambda: called.__setitem__("migrations", 1))
    monkeypatch.setattr(run, "create_pipeline_run", lambda: called.__setitem__("create_run", 1))
    monkeypatch.setattr(run, "load_to_postgres", lambda *_args, **_kwargs: called.__setitem__("load", 1))
    monkeypatch.setattr(
        run,
        "finalize_pipeline_run",
        lambda *_args, **_kwargs: called.__setitem__("finalize", 1),
    )

    run.run_pipeline()

    assert called == {"migrations": 0, "create_run": 0, "load": 0, "finalize": 0}
