from __future__ import annotations

import sys
import types

import pytest

from r2g.connectors.snowflake import SnowflakeConnector, _parse_snowflake_url

# ── URL parsing ──────────────────────────────────────────────────────────


class TestParseSnowflakeUrl:
    def test_full_url_parses_every_field(self):
        kw = _parse_snowflake_url(
            "snowflake://svc:hunter2@xy12345.us-east-1/ANALYTICS/CORE"
            "?warehouse=ETL_WH&role=R2G_READER"
        )
        assert kw["user"] == "svc"
        assert kw["password"] == "hunter2"
        assert kw["account"] == "xy12345.us-east-1"
        assert kw["database"] == "ANALYTICS"
        assert kw["warehouse"] == "ETL_WH"
        assert kw["role"] == "R2G_READER"
        assert kw["_url_schema"] == "CORE"

    def test_url_without_schema_omits_url_schema_key(self):
        kw = _parse_snowflake_url(
            "snowflake://svc:hunter2@xy12345/ANALYTICS?warehouse=WH"
        )
        assert kw["database"] == "ANALYTICS"
        assert "_url_schema" not in kw

    def test_percent_encoded_password_is_decoded(self):
        kw = _parse_snowflake_url(
            "snowflake://svc:a%40b%2Fc@xy12345/DB"
        )
        assert kw["password"] == "a@b/c"

    def test_non_snowflake_scheme_is_rejected(self):
        with pytest.raises(ValueError, match="snowflake://"):
            _parse_snowflake_url("postgresql://u:p@h/db")

    def test_missing_database_is_rejected(self):
        with pytest.raises(ValueError, match="database"):
            _parse_snowflake_url("snowflake://svc:x@xy12345")

    def test_missing_user_is_rejected(self):
        with pytest.raises(ValueError, match="user and account"):
            _parse_snowflake_url("snowflake://:x@xy12345/DB")

    def test_blank_string_is_rejected(self):
        with pytest.raises(ValueError):
            _parse_snowflake_url("")


# ── Constructor behavior ────────────────────────────────────────────────


class TestSnowflakeConnectorInit:
    def test_schema_name_defaults_from_url(self):
        conn = SnowflakeConnector(
            "snowflake://svc:x@xy12345/ANALYTICS/CORE",
            schema_name="PUBLIC",
        )
        assert conn.schema_name == "CORE"
        assert conn._database == "ANALYTICS"

    def test_explicit_schema_argument_wins_over_url_schema_when_non_public(self):
        conn = SnowflakeConnector(
            "snowflake://svc:x@xy12345/ANALYTICS/CORE",
            schema_name="REPORTING",
        )
        assert conn.schema_name == "REPORTING"

    def test_schema_normalized_to_uppercase(self):
        conn = SnowflakeConnector(
            "snowflake://svc:x@xy12345/ANALYTICS",
            schema_name="reporting",
        )
        assert conn.schema_name == "REPORTING"

    def test_missing_database_raises(self):
        with pytest.raises(ValueError):
            SnowflakeConnector("snowflake://svc:x@xy12345")


# ── Introspection with a fake driver ────────────────────────────────────


class _FakeCursor:
    def __init__(self, scripted: dict[str, tuple[list[str], list[tuple]]]) -> None:
        self._scripted = scripted
        self.description: list[tuple] | None = None
        self._rows: list[tuple] = []

    def execute(self, sql: str, params=None) -> None:
        key = _match_script(sql, self._scripted)
        if key is None:
            raise AssertionError(f"Unexpected SQL: {sql}")
        columns, rows = self._scripted[key]
        self.description = [(c,) for c in columns]
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)

    def close(self):
        pass


def _match_script(sql: str, scripted: dict[str, tuple]) -> str | None:
    sql_up = " ".join(sql.upper().split())
    for key in scripted:
        if key in sql_up:
            return key
    return None


class _FakeConnection:
    def __init__(self, scripted: dict[str, tuple]) -> None:
        self._scripted = scripted
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._scripted)

    def close(self) -> None:
        self.closed = True


def _install_fake_snowflake(monkeypatch, connect_fn):
    mod = types.ModuleType("snowflake")
    conn_mod = types.ModuleType("snowflake.connector")
    conn_mod.connect = connect_fn  # type: ignore[attr-defined]
    mod.connector = conn_mod  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "snowflake", mod)
    monkeypatch.setitem(sys.modules, "snowflake.connector", conn_mod)


class TestIntrospection:
    def _scripted_schema(self) -> dict[str, tuple]:
        return {
            # tables list
            "INFORMATION_SCHEMA.TABLES": (
                ["TABLE_NAME"],
                [("USERS",), ("ORDERS",)],
            ),
            # columns for any table
            "INFORMATION_SCHEMA.COLUMNS": (
                ["COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"],
                [
                    ("ID", "NUMBER", "NO"),
                    ("NAME", "TEXT", "YES"),
                    ("METADATA", "VARIANT", "YES"),
                    ("CREATED_AT", "TIMESTAMP_NTZ", "YES"),
                ],
            ),
            # SHOW PRIMARY KEYS
            "SHOW PRIMARY KEYS": (
                ["created_on", "database_name", "schema_name", "table_name",
                 "column_name", "key_sequence", "constraint_name"],
                [("x", "ANALYTICS", "CORE", "USERS", "ID", 1, "PK_USERS")],
            ),
            # SHOW IMPORTED KEYS
            "SHOW IMPORTED KEYS": (
                ["created_on", "pk_database_name", "pk_schema_name", "pk_table_name",
                 "pk_column_name", "fk_database_name", "fk_schema_name",
                 "fk_table_name", "fk_column_name", "key_sequence",
                 "update_rule", "delete_rule", "fk_name", "pk_name", "deferrability"],
                [],
            ),
        }

    def test_get_schema_populates_tables_columns_and_pks(self, monkeypatch):
        scripted = self._scripted_schema()
        captured_kwargs: dict = {}

        def fake_connect(**kwargs):
            captured_kwargs.update(kwargs)
            return _FakeConnection(scripted)

        _install_fake_snowflake(monkeypatch, fake_connect)

        conn = SnowflakeConnector(
            "snowflake://svc:xxx@xy12345/ANALYTICS/CORE"
            "?warehouse=ETL_WH&role=R2G_READER"
        )
        schema = conn.get_schema()

        assert set(schema.tables.keys()) == {"USERS", "ORDERS"}
        users = schema.tables["USERS"]
        assert [c.name for c in users.columns] == ["ID", "NAME", "METADATA", "CREATED_AT"]
        assert users.primary_key == ["ID"]
        assert next(c for c in users.columns if c.name == "ID").is_primary_key is True
        assert next(c for c in users.columns if c.name == "NAME").is_primary_key is False
        assert next(c for c in users.columns if c.name == "NAME").is_nullable is True
        assert users.foreign_keys == []

        assert captured_kwargs["user"] == "svc"
        assert captured_kwargs["account"] == "xy12345"
        assert captured_kwargs["database"] == "ANALYTICS"
        assert captured_kwargs["schema"] == "CORE"
        assert captured_kwargs["warehouse"] == "ETL_WH"
        assert captured_kwargs["role"] == "R2G_READER"
        assert "_url_schema" not in captured_kwargs

    def test_composite_fk_is_grouped_and_ordered(self, monkeypatch):
        # In Snowflake SHOW IMPORTED KEYS, the current table is the FK
        # role and the referenced table is the PK role. Rows are
        # returned in arbitrary order; our code must sort by
        # KEY_SEQUENCE. Here we introspect ORDER_LINES, which imports
        # (ORDER_ID, SUB_ID) → ORDERS(ORDER_ID, SUB_ID).
        scripted = self._scripted_schema()
        scripted["SHOW IMPORTED KEYS"] = (
            ["created_on", "pk_database_name", "pk_schema_name", "pk_table_name",
             "pk_column_name", "fk_database_name", "fk_schema_name",
             "fk_table_name", "fk_column_name", "key_sequence",
             "update_rule", "delete_rule", "fk_name", "pk_name", "deferrability"],
            [
                # intentionally out-of-order to test sort-by-key_sequence
                ("x", "A", "C", "ORDERS", "SUB_ID", "A", "C", "ORDER_LINES",
                 "SUB_ID", 2, "", "", "FK_OL_ORDER", "PK_ORDERS", ""),
                ("x", "A", "C", "ORDERS", "ORDER_ID", "A", "C", "ORDER_LINES",
                 "ORDER_ID", 1, "", "", "FK_OL_ORDER", "PK_ORDERS", ""),
            ],
        )

        def fake_connect(**kwargs):
            return _FakeConnection(scripted)

        _install_fake_snowflake(monkeypatch, fake_connect)

        conn = SnowflakeConnector("snowflake://svc:x@xy12345/ANALYTICS/CORE")
        schema = conn.get_schema()
        fks = schema.tables["USERS"].foreign_keys
        assert len(fks) == 1
        fk = fks[0]
        assert fk.columns == ["ORDER_ID", "SUB_ID"]
        assert fk.foreign_columns == ["ORDER_ID", "SUB_ID"]
        assert fk.foreign_table == "ORDERS"
        assert fk.constraint_name == "FK_OL_ORDER"

    def test_missing_driver_surfaces_install_hint(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "snowflake", None)
        monkeypatch.setitem(sys.modules, "snowflake.connector", None)
        conn = SnowflakeConnector("snowflake://svc:x@xy12345/ANALYTICS")
        with pytest.raises(ImportError, match="r2g-arango\\[snowflake\\]"):
            conn.get_schema()

    def test_driver_exception_is_wrapped_as_runtime_error(self, monkeypatch):
        def fake_connect(**kwargs):
            raise RuntimeError("boom from snowflake")

        _install_fake_snowflake(monkeypatch, fake_connect)
        conn = SnowflakeConnector("snowflake://svc:x@xy12345/ANALYTICS")
        with pytest.raises(RuntimeError, match="Failed to connect to Snowflake"):
            conn.get_schema()

    def test_variant_and_array_columns_round_trip_as_json_types(self, monkeypatch):
        from r2g.config import pg_type_to_json_type

        scripted = self._scripted_schema()

        def fake_connect(**kwargs):
            return _FakeConnection(scripted)

        _install_fake_snowflake(monkeypatch, fake_connect)

        conn = SnowflakeConnector("snowflake://svc:x@xy12345/ANALYTICS/CORE")
        schema = conn.get_schema()
        users = schema.tables["USERS"]
        metadata_col = next(c for c in users.columns if c.name == "METADATA")
        assert pg_type_to_json_type(metadata_col.data_type) == "object"
        created_at = next(c for c in users.columns if c.name == "CREATED_AT")
        assert pg_type_to_json_type(created_at.data_type) == "string"
        id_col = next(c for c in users.columns if c.name == "ID")
        assert pg_type_to_json_type(id_col.data_type) == "float"
