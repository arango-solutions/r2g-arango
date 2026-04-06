"""Unit tests for the config_migrate module."""
from __future__ import annotations

from r2g.config import ConfigManager
from r2g.config_migrate import MigrationReport, migrate_config
from r2g.types import (
    Column,
    ForeignKey,
    Schema,
    Table,
)


def _schema(**tables_spec) -> Schema:
    tables = {}
    for name, spec in tables_spec.items():
        cols = [
            Column(name=c[0], data_type=c[1], is_nullable=c[2], is_primary_key=c[3])
            for c in spec.get("columns", [])
        ]
        fks = [
            ForeignKey(column=f[0], foreign_table=f[1], foreign_column=f[2])
            for f in spec.get("fks", [])
        ]
        tables[name] = Table(
            name=name,
            columns=cols,
            primary_key=spec.get("pk", []),
            foreign_keys=fks,
        )
    return Schema(tables=tables)


class TestMigrationReport:
    def test_no_changes(self):
        r = MigrationReport()
        assert not r.has_changes

    def test_has_changes_added_collections(self):
        r = MigrationReport()
        r.added_collections.append("x")
        assert r.has_changes

    def test_has_changes_orphaned(self):
        r = MigrationReport()
        r.orphaned_collections.append("x")
        assert r.has_changes


class TestMigrateConfigNoChange:
    def test_identical_schema_no_changes(self):
        s = _schema(
            users={
                "columns": [("id", "integer", False, True), ("name", "text", False, False)],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(s)
        updated, report = migrate_config(config, s)
        assert not report.has_changes
        assert len(updated.collections) == len(config.collections)
        assert len(updated.edges) == len(config.edges)


class TestMigrateConfigAddedTable:
    def test_new_table_added(self):
        old_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        new_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
            products={
                "columns": [("id", "integer", False, True), ("name", "text", False, False)],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        updated, report = migrate_config(config, new_schema)
        assert "products" in report.added_collections
        assert "products" in updated.collections
        assert updated.collections["products"].source_table == "products"


class TestMigrateConfigOrphanedTable:
    def test_removed_table_flagged(self):
        old_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
            legacy={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        new_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        updated, report = migrate_config(config, new_schema)
        assert "legacy" in report.orphaned_collections
        # orphaned collections are flagged but NOT removed
        assert "legacy" in updated.collections


class TestMigrateConfigAddedEdge:
    def test_new_fk_creates_edge(self):
        old_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
            orders={
                "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
                "pk": ["id"],
            },
        )
        new_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
            orders={
                "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
                "pk": ["id"],
                "fks": [("user_id", "users", "id")],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        assert len(config.edges) == 0
        updated, report = migrate_config(config, new_schema)
        assert len(updated.edges) == 1
        assert "orders_to_users" in report.added_edges


class TestMigrateConfigRemovedEdge:
    def test_dropped_fk_removes_edge(self):
        old_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
            orders={
                "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
                "pk": ["id"],
                "fks": [("user_id", "users", "id")],
            },
        )
        new_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
            orders={
                "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        assert len(config.edges) == 1
        updated, report = migrate_config(config, new_schema)
        assert len(updated.edges) == 0
        assert len(report.removed_edges) == 1


class TestMigrateConfigFieldCleanup:
    def test_dropped_column_cleans_field_mapping(self):
        old_schema = _schema(
            users={
                "columns": [
                    ("id", "integer", False, True),
                    ("name", "text", False, False),
                    ("legacy_field", "text", False, False),
                ],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        config.collections["users"].field_mappings = {"name": "user_name", "legacy_field": "old"}

        new_schema = _schema(
            users={
                "columns": [
                    ("id", "integer", False, True),
                    ("name", "text", False, False),
                ],
                "pk": ["id"],
            },
        )
        updated, report = migrate_config(config, new_schema)
        assert "legacy_field" not in updated.collections["users"].field_mappings
        assert "name" in updated.collections["users"].field_mappings
        assert any("legacy_field" in f for f in report.cleaned_fields)

    def test_dropped_column_cleans_exclude_fields(self):
        old_schema = _schema(
            users={
                "columns": [
                    ("id", "integer", False, True),
                    ("name", "text", False, False),
                    ("secret", "text", False, False),
                ],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        config.collections["users"].exclude_fields = ["secret"]

        new_schema = _schema(
            users={
                "columns": [
                    ("id", "integer", False, True),
                    ("name", "text", False, False),
                ],
                "pk": ["id"],
            },
        )
        updated, report = migrate_config(config, new_schema)
        assert "secret" not in updated.collections["users"].exclude_fields

    def test_dropped_column_cleans_include_fields(self):
        old_schema = _schema(
            users={
                "columns": [
                    ("id", "integer", False, True),
                    ("name", "text", False, False),
                    ("bio", "text", False, False),
                ],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        config.collections["users"].include_fields = ["id", "name", "bio"]

        new_schema = _schema(
            users={
                "columns": [
                    ("id", "integer", False, True),
                    ("name", "text", False, False),
                ],
                "pk": ["id"],
            },
        )
        updated, report = migrate_config(config, new_schema)
        assert "bio" not in updated.collections["users"].include_fields
        assert updated.collections["users"].include_fields == ["id", "name"]

    def test_include_fields_set_to_none_when_all_removed(self):
        old_schema = _schema(
            users={
                "columns": [
                    ("id", "integer", False, True),
                    ("temp1", "text", False, False),
                ],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        config.collections["users"].include_fields = ["temp1"]

        new_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        updated, report = migrate_config(config, new_schema)
        assert updated.collections["users"].include_fields is None


class TestMigrateConfigTypeOverrides:
    def test_stale_type_override_removed(self):
        old_schema = _schema(
            users={
                "columns": [
                    ("id", "integer", False, True),
                    ("score", "numeric", False, False),
                ],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        config.type_overrides = {
            "users.score": "integer",
            "users.legacy": "string",
        }

        new_schema = _schema(
            users={
                "columns": [
                    ("id", "integer", False, True),
                    ("score", "numeric", False, False),
                ],
                "pk": ["id"],
            },
        )
        updated, report = migrate_config(config, new_schema)
        assert "users.score" in updated.type_overrides
        assert "users.legacy" not in updated.type_overrides
        assert any("users.legacy" in f for f in report.cleaned_fields)

    def test_type_override_for_dropped_table_removed(self):
        old_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        config.type_overrides = {"gone_table.col": "string"}

        new_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        updated, report = migrate_config(config, new_schema)
        assert "gone_table.col" not in updated.type_overrides


class TestMigrateConfigPreservesCustomizations:
    def test_renamed_collection_preserved(self):
        old_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        config.collections["users"].target_collection = "app_users"

        updated, report = migrate_config(config, old_schema)
        assert updated.collections["users"].target_collection == "app_users"

    def test_key_separator_preserved(self):
        schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(schema)
        config.key_separator = ":"
        updated, report = migrate_config(config, schema)
        assert updated.key_separator == ":"


class TestMigrateConfigSourceSchema:
    def test_source_schema_override(self):
        schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(schema)
        assert config.source_schema == "public"
        updated, _ = migrate_config(config, schema, source_schema="sales")
        assert updated.source_schema == "sales"

    def test_source_schema_unchanged_when_none(self):
        schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(schema)
        config.source_schema = "custom"
        updated, _ = migrate_config(config, schema, source_schema=None)
        assert updated.source_schema == "custom"


class TestMigrateConfigOriginalNotMutated:
    def test_deepcopy_preserves_original(self):
        schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        new_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
            products={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(schema)
        original_keys = set(config.collections.keys())
        migrate_config(config, new_schema)
        assert set(config.collections.keys()) == original_keys


class TestMigrateConfigEdgeDedup:
    def test_duplicate_edge_names_disambiguated(self):
        old_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
            orders={
                "columns": [
                    ("id", "integer", False, True),
                    ("user_id", "integer", False, False),
                    ("approver_id", "integer", False, False),
                ],
                "pk": ["id"],
                "fks": [("user_id", "users", "id")],
            },
        )
        new_schema = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
            orders={
                "columns": [
                    ("id", "integer", False, True),
                    ("user_id", "integer", False, False),
                    ("approver_id", "integer", False, False),
                ],
                "pk": ["id"],
                "fks": [
                    ("user_id", "users", "id"),
                    ("approver_id", "users", "id"),
                ],
            },
        )
        config = ConfigManager.generate_default_config(old_schema)
        updated, report = migrate_config(config, new_schema)
        edge_names = [e.edge_collection for e in updated.edges]
        assert len(edge_names) == 2
        assert len(set(edge_names)) == 2  # all unique
        assert len(report.added_edges) == 1
