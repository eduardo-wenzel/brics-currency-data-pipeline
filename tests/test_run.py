from pipeline.run import _should_skip_db_load


def test_should_skip_db_load_true(monkeypatch):
    monkeypatch.setenv("SKIP_DB_LOAD", "true")
    assert _should_skip_db_load() is True


def test_should_skip_db_load_false(monkeypatch):
    monkeypatch.setenv("SKIP_DB_LOAD", "false")
    assert _should_skip_db_load() is False
