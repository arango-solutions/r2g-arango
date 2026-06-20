from __future__ import annotations

import sys
import types

import pytest

from r2g.connectors.mssql import SQLServerConnector, _parse_mssql_url

# ── URL parsing ──────────────────────────────────────────────────────────


class TestParseMssqlUrl:
    def test_full_url_parses_every_field(self):
        kw = _parse_mssql_url("mssql://svc:hunter2@db.host:1444/shop")
        assert kw["user"] == "svc"
        assert kw["password"] == "hunter2"
        assert kw["server"] == "db.host"
        assert kw["port"] == 1444
        assert kw["database"] == "shop"

    def test_default_port_is_1433(self):
        assert _parse_mssql_url("mssql://u:p@h/db")["port"] == 1433

    def test_sqlserver_scheme_accepted(self):
        assert _parse_mssql_url("sqlserver://u:p@h/db")["database"] == "db"

    def test_driver_suffix_scheme_tolerated(self):
        assert _parse_mssql_url("mssql+pymssql://u:p@h/db")["server"] == "h"

    def test_percent_encoded_password_is_decoded(self):
        assert _parse_mssql_url("mssql://u:a%40b%2Fc@h/db")["password"] == "a@b/c"

    def test_wrong_scheme_rejected(self):
        with pytest.raises(ValueError, match="mssql:// or sqlserver://"):
            _parse_mssql_url("postgresql://u:p@h/db")

    def test_missing_database_rejected(self):
        with pytest.raises(ValueError, match="database"):
            _parse_mssql_url("mssql://u:p@h")

    def test_missing_user_rejected(self):
        with pytest.raises(ValueError, match="user and host"):
            _parse_mssql_url("mssql://:p@h/db")

    def test_blank_rejected(self):
        with pytest.raises(ValueError):
            _parse_mssql_url("")


class TestSqlServerConnectorInit:
    def test_schema_defaults_to_dbo(self):
        conn = SQLServerConnector("mssql://u:p@h/shop")
        assert conn.schema_name == "dbo"
        assert conn._database == "shop"

    def test_public_sentinel_folds_to_dbo(self):
        # `source snapshot` passes the PG default "public"; SQL Server uses dbo.
        assert SQLServerConnector("mssql://u:p@h/shop", schema_name="public").schema_name == "dbo"

    def test_explicit_schema_is_kept(self):
        assert SQLServerConnector("mssql://u:p@h/shop", schema_name="sales").schema_name == "sales"


# ── Fake pymssql driver ──────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, db: dict, as_dict: bool = False) -> None:
        self._db = db
        self._as_dict = as_dict
        self._rows: list = []
        self.description = None

    def execute(self, sql, params=None):
        s = " ".join(sql.upper().split())
        if "INFORMATION_SCHEMA.TABLES" in s:
            self._rows = [{"TABLE_NAME": t} for t in self._db["tables"]]
        elif "INFORMATION_SCHEMA.COLUMNS" in s:
            self._rows = self._db["columns"][params[1]]
        elif "PRIMARY KEY" in s:
            self._rows = [{"COLUMN_NAME": c} for c in self._db["pks"].get(params[1], [])]
        elif "SYS.FOREIGN_KEYS" in s:
            self._rows = self._db["fks"].get(params[1], [])
        elif "COUNT(*)" in s:
            self._rows = [(self._db.get("count", 0),)]
        else:
            self._rows = []

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


class _FakeConnection:
    def __init__(self, db: dict) -> None:
        self._db = db
        self.closed = False

    def cursor(self, as_dict: bool = False):
        return _FakeCursor(self._db, as_dict=as_dict)

    def close(self):
        self.closed = True


def _install_fake_pymssql(monkeypatch, connect_fn):
    mod = types.ModuleType("pymssql")
    mod.connect = connect_fn
    monkeypatch.setitem(sys.modules, "pymssql", mod)


def _sample_db() -> dict:
    return {
        "tables": ["users", "orders", "order_items"],
        "columns": {
            "users": [
                {"COLUMN_NAME": "id", "DATA_TYPE": "INT", "IS_NULLABLE": "NO"},
                {"COLUMN_NAME": "name", "DATA_TYPE": "NVARCHAR", "IS_NULLABLE": "YES"},
                {"COLUMN_NAME": "is_active", "DATA_TYPE": "BIT", "IS_NULLABLE": "NO"},
            ],
            "orders": [
                {"COLUMN_NAME": "order_id", "DATA_TYPE": "bigint", "IS_NULLABLE": "NO"},
                {"COLUMN_NAME": "user_id", "DATA_TYPE": "int", "IS_NULLABLE": "NO"},
            ],
            "order_items": [
                {"COLUMN_NAME": "order_id", "DATA_TYPE": "bigint", "IS_NULLABLE": "NO"},
                {"COLUMN_NAME": "product_id", "DATA_TYPE": "int", "IS_NULLABLE": "NO"},
            ],
        },
        "pks": {
            "users": ["id"],
            "orders": ["order_id"],
            "order_items": ["order_id", "product_id"],
        },
        "fks": {
            "orders": [
                {
                    "constraint_name": "fk_orders_user",
                    "column_name": "user_id",
                    "foreign_table_name": "users",
                    "foreign_column_name": "id",
                },
            ],
        },
    }


class TestIntrospection:
    def test_get_schema_tables_columns_pks(self, monkeypatch):
        captured = {}

        def fake_connect(**kwargs):
            captured.update(kwargs)
            return _FakeConnection(_sample_db())

        _install_fake_pymssql(monkeypatch, fake_connect)
        schema = SQLServerConnector("mssql://svc:x@h:1433/shop").get_schema()

        assert set(schema.tables) == {"users", "orders", "order_items"}
        users = schema.tables["users"]
        assert users.primary_key == ["id"]
        assert next(c for c in users.columns if c.name == "name").data_type == "nvarchar"
        assert captured["database"] == "shop"
        assert captured["server"] == "h"

    def test_bit_is_translated_to_boolean(self, monkeypatch):
        _install_fake_pymssql(monkeypatch, lambda **kw: _FakeConnection(_sample_db()))
        schema = SQLServerConnector("mssql://u:p@h/shop").get_schema()
        is_active = next(c for c in schema.tables["users"].columns if c.name == "is_active")
        assert is_active.data_type == "boolean"

    def test_composite_pk_order_preserved(self, monkeypatch):
        _install_fake_pymssql(monkeypatch, lambda **kw: _FakeConnection(_sample_db()))
        schema = SQLServerConnector("mssql://u:p@h/shop").get_schema()
        assert schema.tables["order_items"].primary_key == ["order_id", "product_id"]

    def test_foreign_key_grouped(self, monkeypatch):
        _install_fake_pymssql(monkeypatch, lambda **kw: _FakeConnection(_sample_db()))
        schema = SQLServerConnector("mssql://u:p@h/shop").get_schema()
        fks = schema.tables["orders"].foreign_keys
        assert len(fks) == 1
        assert fks[0].columns == ["user_id"]
        assert fks[0].foreign_table == "users"
        assert fks[0].foreign_columns == ["id"]


class TestSession:
    def test_count_rows(self, monkeypatch):
        db = _sample_db()
        db["count"] = 7
        _install_fake_pymssql(monkeypatch, lambda **kw: _FakeConnection(db))
        session = SQLServerConnector("mssql://u:p@h/shop").open_session()
        assert session.count_rows("users") == 7
        session.close()

    def test_qualified_uses_brackets(self, monkeypatch):
        _install_fake_pymssql(monkeypatch, lambda **kw: _FakeConnection(_sample_db()))
        session = SQLServerConnector("mssql://u:p@h/shop", schema_name="sales").open_session()
        assert session._qualified("users") == "[sales].[users]"
