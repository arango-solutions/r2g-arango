"""Tests for the ClickHouse source connector (introspection + ETL session).

Pure-logic + fake-client unit tests, plus opt-in LIVE tests that exercise the
real clickhouse-connect API (introspect / count / stream / dump) — the fake
can't prove the driver calls are correct (see the arango-sparql-py
cross-validation lesson).
"""

from __future__ import annotations

import os

import pytest

from r2g.connectors.base import create_source_connector
from r2g.connectors.clickhouse import (
    ClickHouseConnector,
    ClickHouseSession,
    _clean_type,
    _database_of,
)

# -- pure logic --------------------------------------------------------------


@pytest.mark.parametrize(
    "ch_type,expected",
    [
        ("String", "string"),
        ("Nullable(String)", "string"),
        ("LowCardinality(String)", "string"),
        ("LowCardinality(Nullable(String))", "string"),
        ("UInt64", "uint64"),
        ("DateTime64(3)", "datetime64(3)"),
        ("Float64", "float64"),
    ],
)
def test_clean_type(ch_type, expected):
    assert _clean_type(ch_type) == expected


def test_database_of():
    assert _database_of("clickhouse://u:p@h:8123/analytics") == "analytics"
    assert _database_of("clickhouse://u:p@h:8123") == "default"


def test_factory_dispatches_clickhouse():
    conn = create_source_connector("clickhouse", "clickhouse://u:p@h:8123/analytics")
    assert isinstance(conn, ClickHouseConnector)
    assert conn.schema_name == "analytics"  # ClickHouse namespace == database


# -- introspection with a fake client ---------------------------------------


class _QR:
    def __init__(self, rows, columns=None):
        self.result_rows = rows
        self.column_names = columns or []


class _Stream:
    def __init__(self, blocks):
        self._blocks = blocks

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __iter__(self):
        return iter(self._blocks)


class _FakeClient:
    """Routes queries by SQL content; ``data`` is table -> list[row-tuple]."""

    def __init__(self, tables, columns, data=None):
        self._tables = tables
        self._columns = columns  # table -> [(name, type, is_in_pk), ...]
        self._data = data or {}
        self.closed = False

    def query(self, sql, parameters=None):
        if "system.tables" in sql:
            return _QR([(t,) for t in self._tables])
        if "system.columns" in sql:
            return _QR(self._columns[parameters["tbl"]])
        if sql.startswith("DESCRIBE"):
            table = sql.split("`")[-2]
            return _QR([(c[0],) for c in self._columns[table]])
        if "count()" in sql:
            table = sql.split("`")[-2]
            return _QR([(len(self._data.get(table, [])),)])
        raise AssertionError(f"unexpected query: {sql}")

    def query_row_block_stream(self, sql, parameters=None):
        table = sql.split("`")[-2]
        return _Stream([self._data.get(table, [])])

    def close(self):
        self.closed = True


_COLS = {
    "usage_metrics": [
        ("id", "UInt64", 1),
        ("account_id", "String", 0),
        ("edition", "Nullable(String)", 0),
    ]
}


def test_get_schema_parses_columns_and_pk():
    client = _FakeClient(["usage_metrics"], _COLS)
    schema = ClickHouseConnector("clickhouse://u:p@h/analytics", client=client).get_schema()
    assert set(schema.tables) == {"usage_metrics"}
    t = schema.tables["usage_metrics"]
    assert t.primary_key == ["id"]
    by_name = {c.name: c for c in t.columns}
    assert by_name["id"].data_type == "uint64" and by_name["id"].is_primary_key
    assert by_name["edition"].data_type == "string" and by_name["edition"].is_nullable
    assert not by_name["account_id"].is_nullable


def test_session_count_and_stream():
    data = {"usage_metrics": [(1, "ACME", "Enterprise"), (2, "GLOBEX", "Community")]}
    client = _FakeClient(["usage_metrics"], _COLS, data)
    session = ClickHouseSession("clickhouse://u:p@h/analytics", database="analytics", client=client)
    assert session.count_rows("usage_metrics") == 2
    rows = list(session.stream_rows("usage_metrics"))
    assert rows[0] == {"id": 1, "account_id": "ACME", "edition": "Enterprise"}
    session.close()
    assert client.closed


# -- live (opt-in) -----------------------------------------------------------

_DSN = os.getenv("CLICKHOUSE_DSN")
live = pytest.mark.skipif(not _DSN, reason="set CLICKHOUSE_DSN for live ClickHouse tests")


@live
def test_live_introspect_and_stream():
    pytest.importorskip("clickhouse_connect")
    conn = create_source_connector("clickhouse", _DSN)
    schema = conn.get_schema()
    assert "usage_metrics" in schema.tables
    cols = {c.name for c in schema.tables["usage_metrics"].columns}
    assert {"account_id", "edition", "query_volume_m"} <= cols

    session = conn.open_session()
    try:
        assert session.count_rows("usage_metrics") == 3
        rows = list(session.stream_rows("usage_metrics"))
        assert len(rows) == 3
        assert {r["account_id"] for r in rows} >= {"001Qwvb5LAnzy3yVgi"}
    finally:
        session.close()
