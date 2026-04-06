"""Unit tests for the schema_diff module."""
from __future__ import annotations

from r2g.schema_diff import diff_schemas
from r2g.types import Column, ForeignKey, Schema, Table


def _make_schema(**tables_spec) -> Schema:
    tables = {}
    for name, spec in tables_spec.items():
        cols = [Column(name=c[0], data_type=c[1], is_nullable=c[2], is_primary_key=c[3])
                for c in spec.get("columns", [])]
        fks = [ForeignKey(column=f[0], foreign_table=f[1], foreign_column=f[2])
               for f in spec.get("fks", [])]
        tables[name] = Table(
            name=name,
            columns=cols,
            primary_key=spec.get("pk", []),
            foreign_keys=fks,
        )
    return Schema(tables=tables)


class TestDiffSchemas:
    def test_identical_schemas(self):
        s = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        result = diff_schemas(s, s)
        assert result["added_tables"] == []
        assert result["removed_tables"] == []
        assert result["modified_tables"] == {}

    def test_added_table(self):
        old = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        new = _make_schema(
            users={"columns": [("id", "integer", False, True)], "pk": ["id"]},
            orders={"columns": [("id", "integer", False, True)], "pk": ["id"]},
        )
        result = diff_schemas(old, new)
        assert "orders" in result["added_tables"]
        assert result["removed_tables"] == []

    def test_removed_table(self):
        old = _make_schema(
            users={"columns": [("id", "integer", False, True)], "pk": ["id"]},
            orders={"columns": [("id", "integer", False, True)], "pk": ["id"]},
        )
        new = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        result = diff_schemas(old, new)
        assert "orders" in result["removed_tables"]

    def test_added_column(self):
        old = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        new = _make_schema(users={
            "columns": [("id", "integer", False, True), ("name", "text", False, False)],
            "pk": ["id"],
        })
        result = diff_schemas(old, new)
        mods = result["modified_tables"]["users"]
        assert len(mods["added_columns"]) == 1
        assert mods["added_columns"][0]["name"] == "name"

    def test_removed_column(self):
        old = _make_schema(users={
            "columns": [("id", "integer", False, True), ("name", "text", False, False)],
            "pk": ["id"],
        })
        new = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        result = diff_schemas(old, new)
        mods = result["modified_tables"]["users"]
        assert "name" in mods["removed_columns"]

    def test_type_change(self):
        old = _make_schema(users={
            "columns": [("id", "integer", False, True), ("name", "text", False, False)],
            "pk": ["id"],
        })
        new = _make_schema(users={
            "columns": [("id", "integer", False, True), ("name", "varchar(255)", False, False)],
            "pk": ["id"],
        })
        result = diff_schemas(old, new)
        changes = result["modified_tables"]["users"]["type_changes"]
        assert len(changes) == 1
        assert changes[0]["column"] == "name"
        assert changes[0]["old_type"] == "text"
        assert changes[0]["new_type"] == "varchar(255)"

    def test_nullable_change(self):
        old = _make_schema(users={
            "columns": [("id", "integer", False, True), ("email", "text", False, False)],
            "pk": ["id"],
        })
        new = _make_schema(users={
            "columns": [("id", "integer", False, True), ("email", "text", True, False)],
            "pk": ["id"],
        })
        result = diff_schemas(old, new)
        changes = result["modified_tables"]["users"]["nullable_changes"]
        assert changes[0]["column"] == "email"
        assert changes[0]["new_nullable"] is True

    def test_pk_change(self):
        old = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        new = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id", "name"]})
        result = diff_schemas(old, new)
        mods = result["modified_tables"]["users"]
        assert mods["pk_changed"] is True
        assert mods["old_pk"] == ["id"]
        assert mods["new_pk"] == ["id", "name"]

    def test_added_fk(self):
        old = _make_schema(orders={
            "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
            "pk": ["id"],
        })
        new = _make_schema(orders={
            "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
            "pk": ["id"],
            "fks": [("user_id", "users", "id")],
        })
        result = diff_schemas(old, new)
        mods = result["modified_tables"]["orders"]
        assert len(mods["added_fks"]) == 1
        assert mods["added_fks"][0]["foreign_table"] == "users"

    def test_removed_fk(self):
        old = _make_schema(orders={
            "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
            "pk": ["id"],
            "fks": [("user_id", "users", "id")],
        })
        new = _make_schema(orders={
            "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
            "pk": ["id"],
        })
        result = diff_schemas(old, new)
        mods = result["modified_tables"]["orders"]
        assert len(mods["removed_fks"]) == 1

    def test_multiple_changes(self):
        old = _make_schema(
            users={"columns": [("id", "integer", False, True), ("name", "text", False, False)], "pk": ["id"]},
            orders={"columns": [("id", "integer", False, True)], "pk": ["id"]},
        )
        new = _make_schema(
            users={
                "columns": [
                    ("id", "integer", False, True),
                    ("name", "varchar", False, False),
                    ("email", "text", True, False),
                ],
                "pk": ["id"],
            },
            products={"columns": [("id", "integer", False, True)], "pk": ["id"]},
        )
        result = diff_schemas(old, new)
        assert "products" in result["added_tables"]
        assert "orders" in result["removed_tables"]
        mods = result["modified_tables"]["users"]
        assert len(mods["added_columns"]) == 1
        assert len(mods["type_changes"]) == 1
