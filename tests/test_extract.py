import json

from pipeline import extract


def test_save_raw_data_writes_json(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    payload = {"base": "USD", "rates": {"BRL": 5.0}}

    path = extract.save_raw_data(payload)

    assert path.exists()
    assert path.parent.name == "raw"
    with path.open("r", encoding="utf-8") as f:
        saved = json.load(f)
    assert saved == payload


def test_fetch_exchange_rates_success(monkeypatch):
    class DummyResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"base": "USD", "rates": {"BRL": 5.0}}

    monkeypatch.setattr(extract, "BASE_URL", "https://example.com")
    monkeypatch.setattr(extract.requests, "get", lambda *args, **kwargs: DummyResponse())

    data = extract.fetch_exchange_rates()

    assert data["base"] == "USD"
    assert "BRL" in data["rates"]
