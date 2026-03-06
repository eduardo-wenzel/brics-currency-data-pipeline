import json

from pipeline import transform


def test_normalize_raw_payload_filters_currencies(monkeypatch):
    monkeypatch.setattr(transform, "CURRENCIES", "BRL,CNY")
    payload = {
        "base": "usd",
        "date": "2026-03-06",
        "rates": {"BRL": 5.0, "CNY": 7.2, "EUR": 0.9},
    }

    normalized = transform._normalize_raw_payload(payload)

    assert normalized["base_currency"] == "USD"
    assert set(normalized["rates"].keys()) == {"BRL", "CNY"}


def test_transform_latest_file_returns_dataframe(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    raw_file = raw_dir / "sample.json"
    raw_file.write_text(
        json.dumps(
            {
                "base": "USD",
                "date": "2026-03-06",
                "rates": {"BRL": 5.0, "CNY": 7.2},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(transform, "CURRENCIES", "")
    monkeypatch.setattr(transform, "get_latest_raw_file", lambda: raw_file)

    df = transform.transform_latest_file()

    assert len(df) == 2
    assert set(df.columns) == {"base_currency", "target_currency", "rate", "reference_date"}
    assert set(df["target_currency"]) == {"BRL", "CNY"}
