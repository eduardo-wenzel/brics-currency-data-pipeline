from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from pipeline import migrations


class DummyCursor:
    def __init__(self, applied_versions=None):
        self.executed: list[tuple[str, tuple | None]] = []
        self.applied_versions = applied_versions or []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return [(version,) for version in self.applied_versions]


class DummyConnection:
    def __init__(self, cursor: DummyCursor):
        self._cursor = cursor
        self.commit_calls = 0
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commit_calls += 1

    def close(self):
        self.closed = True


def _make_migration_dir(name: str) -> Path:
    migration_dir = Path("data/test_tmp") / f"{name}-{uuid4().hex}"
    migration_dir.mkdir(parents=True, exist_ok=True)
    return migration_dir


def test_apply_migrations_runs_pending_files(monkeypatch):
    migration_dir = _make_migration_dir("migrations-pending")
    migration_file = migration_dir / "001_create_tables.sql"
    migration_file.write_text("CREATE TABLE demo(id INT);", encoding="utf-8")
    cursor = DummyCursor()
    conn = DummyConnection(cursor)

    monkeypatch.setattr(migrations, "_MIGRATIONS_APPLIED", False)
    monkeypatch.setattr(migrations, "MIGRATIONS_DIR", migration_dir)
    monkeypatch.setattr(migrations, "_get_connection", lambda: conn)

    migrations.apply_migrations()

    executed_queries = [query.strip() for query, _params in cursor.executed]
    insert_calls = [(query.strip(), params) for query, params in cursor.executed]

    assert any("CREATE TABLE IF NOT EXISTS public.schema_migrations" in query for query in executed_queries)
    assert "SELECT version FROM public.schema_migrations;" in executed_queries
    assert "CREATE TABLE demo(id INT);" in executed_queries
    assert (
        "INSERT INTO public.schema_migrations (version)\n                    VALUES (%s);".strip(),
        ("001_create_tables.sql",),
    ) in insert_calls
    assert conn.commit_calls == 1


def test_apply_migrations_skips_when_already_applied(monkeypatch):
    migration_dir = _make_migration_dir("migrations-skipped")
    migration_file = migration_dir / "001_create_tables.sql"
    migration_file.write_text("CREATE TABLE demo(id INT);", encoding="utf-8")
    cursor = DummyCursor(applied_versions=["001_create_tables.sql"])
    conn = DummyConnection(cursor)

    monkeypatch.setattr(migrations, "_MIGRATIONS_APPLIED", False)
    monkeypatch.setattr(migrations, "MIGRATIONS_DIR", migration_dir)
    monkeypatch.setattr(migrations, "_get_connection", lambda: conn)

    migrations.apply_migrations()

    executed_queries = [query.strip() for query, _params in cursor.executed]

    assert "CREATE TABLE demo(id INT);" not in executed_queries
    assert conn.commit_calls == 1


def test_apply_migrations_runs_once_per_process(monkeypatch):
    migration_dir = _make_migration_dir("migrations-once")
    migration_file = migration_dir / "001_create_tables.sql"
    migration_file.write_text("CREATE TABLE demo(id INT);", encoding="utf-8")
    first_cursor = DummyCursor()
    second_cursor = DummyCursor()
    first_conn = DummyConnection(first_cursor)
    second_conn = DummyConnection(second_cursor)
    connections = iter([first_conn, second_conn])

    monkeypatch.setattr(migrations, "_MIGRATIONS_APPLIED", False)
    monkeypatch.setattr(migrations, "MIGRATIONS_DIR", migration_dir)
    monkeypatch.setattr(migrations, "_get_connection", lambda: next(connections))

    migrations.apply_migrations()
    migrations.apply_migrations()
    migrations.apply_migrations(force=True)

    assert first_conn.commit_calls == 1
    assert second_conn.commit_calls == 1
