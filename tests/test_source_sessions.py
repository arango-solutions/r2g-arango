"""Tests for the Phase 6 slice 3 bulk-read session abstraction.

Covers :class:`PostgresSession` (smoke: psycopg integration surface via
``monkeypatch``) and :class:`SnowflakeSession` (against an in-memory
fake of ``snowflake.connector``). Both implement the
:class:`r2g.connectors.session.SourceSession` Protocol.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from r2g.connectors.base import SourceConnector
from r2g.connectors.postgres import PostgresConnector, PostgresSession
from r2g.connectors.session import SourceSession
from r2g.connectors.snowflake import SnowflakeConnector, SnowflakeSession

# ── Shared fake Snowflake driver ─────────────────────────────────────


class _FakeSFCursor:
    """Fake snowflake cursor with scripted query responses."""

    def __init__(self, scripts: list[tuple[str, list[str], list[tuple]]]) -> None:
        self._scripts = scripts
        self.executed: list[tuple[str, tuple | None]] = []
        self.description: list[tuple] | None = None
        self._rows: list[tuple] = []
        self._idx = 0
        self.closed = False

    def execute(self, sql: str, params=None) -> None:
        self.executed.append((sql, params))
        sql_up = " ".join(sql.upper().split())
        for key, cols, rows in self._scripts:
            if key.upper() in sql_up:
                self.description = [(c,) for c in cols]
                self._rows = list(rows)
                self._idx = 0
                return
        # Unmatched queries produce empty results (BEGIN/COMMIT fall here).
        self.description = []
        self._rows = []
        self._idx = 0

    def fetchone(self):
        if self._idx >= len(self._rows):
            return None
        row = self._rows[self._idx]
        self._idx += 1
        return row

    def fetchmany(self, size: int):
        out = self._rows[self._idx : self._idx + size]
        self._idx += len(out)
        return out

    def fetchall(self):
        out = self._rows[self._idx :]
        self._idx = len(self._rows)
        return out

    def close(self):
        self.closed = True


class _FakeSFConnection:
    def __init__(self, scripts: list[tuple[str, list[str], list[tuple]]]) -> None:
        self._scripts = scripts
        self.closed = False
        self.cursors: list[_FakeSFCursor] = []

    def cursor(self) -> _FakeSFCursor:
        c = _FakeSFCursor(self._scripts)
        self.cursors.append(c)
        return c

    def close(self) -> None:
        self.closed = True


def _install_fake_snowflake(monkeypatch, connect_fn):
    mod = types.ModuleType("snowflake")
    conn_mod = types.ModuleType("snowflake.connector")
    conn_mod.connect = connect_fn  # type: ignore[attr-defined]
    mod.connector = conn_mod  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "snowflake", mod)
    monkeypatch.setitem(sys.modules, "snowflake.connector", conn_mod)


# ── Protocol conformance ─────────────────────────────────────────────


class TestProtocolConformance:
    def test_postgres_connector_satisfies_source_connector(self):
        c = PostgresConnector("postgresql://localhost/x")
        assert isinstance(c, SourceConnector)

    def test_snowflake_connector_satisfies_source_connector(self):
        c = SnowflakeConnector("snowflake://u:p@xy123/DB/SCH")
        assert isinstance(c, SourceConnector)

    def test_postgres_session_satisfies_source_session(self):
        s = PostgresSession("postgresql://localhost/x")
        assert isinstance(s, SourceSession)

    def test_snowflake_session_satisfies_source_session(self):
        s = SnowflakeSession(
            "snowflake://u:p@xy123/DB/SCH",
            database="DB",
            schema_name="SCH",
            connect_params={"user": "u", "password": "p", "account": "xy123", "database": "DB"},
        )
        assert isinstance(s, SourceSession)


# ── SnowflakeSession behaviour ───────────────────────────────────────


class TestSnowflakeSession:
    def _scripts_for_users(self) -> list[tuple[str, list[str], list[tuple]]]:
        return [
            # Snowflake COUNT(*) path
            ("COUNT(*)", ["count"], [(3,)]),
            # SELECT *
            (
                'SELECT * FROM "DB"."SCH"."USERS"',
                ["ID", "NAME"],
                [(1, "Alice"), (2, "Bob"), (3, None)],
            ),
        ]

    def _make_session(
        self,
        monkeypatch,
        scripts: list[tuple[str, list[str], list[tuple]]],
    ) -> tuple[SnowflakeSession, _FakeSFConnection]:
        fake_conn = _FakeSFConnection(scripts)

        def connect(**kwargs):
            return fake_conn

        _install_fake_snowflake(monkeypatch, connect)
        s = SnowflakeSession(
            "snowflake://u:p@xy123/DB/SCH",
            database="DB",
            schema_name="SCH",
            connect_params={
                "user": "u", "password": "p",
                "account": "xy123", "database": "DB",
            },
        )
        return s, fake_conn

    def test_count_rows(self, monkeypatch):
        s, _ = self._make_session(monkeypatch, self._scripts_for_users())
        try:
            assert s.count_rows("USERS") == 3
        finally:
            s.close()

    def test_count_rows_with_since(self, monkeypatch):
        scripts = [("COUNT(*)", ["count"], [(1,)])]
        s, conn = self._make_session(monkeypatch, scripts)
        try:
            assert s.count_rows("USERS", since_column="CREATED_AT", since_value="2026-01-01") == 1
        finally:
            s.close()
        # Verify the SQL actually used since clause
        executed = [sql for c in conn.cursors for (sql, _) in c.executed]
        assert any('"CREATED_AT" >= %s' in sql for sql in executed), executed

    def test_stream_rows_yields_dicts(self, monkeypatch):
        s, _ = self._make_session(monkeypatch, self._scripts_for_users())
        try:
            rows = list(s.stream_rows("USERS"))
        finally:
            s.close()
        assert rows == [
            {"ID": 1, "NAME": "Alice"},
            {"ID": 2, "NAME": "Bob"},
            {"ID": 3, "NAME": None},
        ]

    def test_dump_table_to_csv_emits_header_and_rows(self, monkeypatch, tmp_path: Path):
        s, _ = self._make_session(monkeypatch, self._scripts_for_users())
        out = tmp_path / "users.csv"
        try:
            n = s.dump_table_to_csv("USERS", out)
        finally:
            s.close()
        assert n == 3
        content = out.read_text()
        lines = content.strip().splitlines()
        assert lines[0] == "ID,NAME"
        assert lines[1] == "1,Alice"
        assert lines[2] == "2,Bob"
        # NULL becomes empty string
        assert lines[3] == "3,"

    def test_close_issues_commit(self, monkeypatch):
        s, conn = self._make_session(monkeypatch, self._scripts_for_users())
        _ = s.connection  # open lazily
        assert conn.closed is False
        s.close()
        assert conn.closed is True
        executed = [sql.upper() for c in conn.cursors for (sql, _) in c.executed]
        assert any("BEGIN" in sql for sql in executed)
        assert any("COMMIT" in sql for sql in executed)

    def test_missing_snowflake_driver_raises_importerror(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "snowflake", None)
        monkeypatch.setitem(sys.modules, "snowflake.connector", None)
        s = SnowflakeSession(
            "snowflake://u:p@xy123/DB/SCH",
            database="DB",
            schema_name="SCH",
            connect_params={"user": "u", "password": "p", "account": "xy123", "database": "DB"},
        )
        with pytest.raises(ImportError, match="r2g\\[snowflake\\]"):
            _ = s.connection

    def test_context_manager_closes_on_exit(self, monkeypatch):
        s, conn = self._make_session(monkeypatch, self._scripts_for_users())
        with s:
            _ = s.connection
        assert conn.closed is True


# ── PostgresSession smoke (psycopg.connect monkey-patched) ───────────


class TestPostgresSessionSmoke:
    def test_sets_repeatable_read_once_per_session(self, monkeypatch):
        import psycopg

        captured: list[str] = []

        fake_conn = MagicMock()
        fake_conn.execute.side_effect = lambda sql, *a, **k: captured.append(str(sql))
        monkeypatch.setattr(psycopg, "connect", lambda *a, **kw: fake_conn)

        s = PostgresSession("postgresql://localhost/x", schema_name="public")
        _ = s.connection
        _ = s.connection  # second access must not reconnect
        assert captured.count("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ") == 1

    def test_close_drops_connection(self, monkeypatch):
        import psycopg

        fake_conn = MagicMock()
        monkeypatch.setattr(psycopg, "connect", lambda *a, **kw: fake_conn)

        s = PostgresSession("postgresql://localhost/x")
        _ = s.connection
        s.close()
        fake_conn.close.assert_called_once()
        assert s._conn is None

    def test_open_session_on_connector_returns_postgres_session(self):
        c = PostgresConnector("postgresql://localhost/x", schema_name="foo")
        s = c.open_session()
        assert isinstance(s, PostgresSession)
        assert s.schema_name == "foo"
        assert s.connection_string == "postgresql://localhost/x"
