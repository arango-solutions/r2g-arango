"""Unit tests for the data_validator module."""
from __future__ import annotations

from r2g.config import ConfigManager
from r2g.data_validator import validate_data
from r2g.types import Column, ForeignKey, Schema, Table


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


class TestValidateDataClean:
    def test_no_orphans(self, tmp_path):
        s = _schema(
            users={
                "columns": [("id", "integer", False, True), ("name", "text", False, False)],
                "pk": ["id"],
            },
            orders={
                "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
                "pk": ["id"],
                "fks": [("user_id", "users", "id")],
            },
        )
        config = ConfigManager.generate_default_config(s)

        (tmp_path / "users.csv").write_text("id,name\n1,Alice\n2,Bob\n")
        (tmp_path / "orders.csv").write_text("id,user_id\n10,1\n20,2\n")

        report = validate_data(s, config, tmp_path)
        assert report.is_clean
        assert report.tables_checked == 1
        assert report.pk_sets_built >= 1
        assert report.rows_scanned == 2

    def test_no_fks_at_all(self, tmp_path):
        s = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
        )
        config = ConfigManager.generate_default_config(s)
        (tmp_path / "users.csv").write_text("id\n1\n2\n")

        report = validate_data(s, config, tmp_path)
        assert report.is_clean
        assert report.tables_checked == 0


class TestValidateDataOrphans:
    def test_orphan_fk_detected(self, tmp_path):
        s = _schema(
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
        config = ConfigManager.generate_default_config(s)

        (tmp_path / "users.csv").write_text("id\n1\n2\n")
        (tmp_path / "orders.csv").write_text("id,user_id\n10,1\n20,999\n")

        report = validate_data(s, config, tmp_path)
        assert not report.is_clean
        assert len(report.issues) == 1
        assert report.issues[0].orphan_value == "999"
        assert report.issues[0].target_table == "users"
        assert report.issues[0].source_table == "orders"

    def test_multiple_orphans(self, tmp_path):
        s = _schema(
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
        config = ConfigManager.generate_default_config(s)

        (tmp_path / "users.csv").write_text("id\n1\n")
        (tmp_path / "orders.csv").write_text("id,user_id\n10,1\n20,5\n30,6\n")

        report = validate_data(s, config, tmp_path)
        assert len(report.issues) == 2


class TestValidateDataNulls:
    def test_null_fk_skipped(self, tmp_path):
        s = _schema(
            users={
                "columns": [("id", "integer", False, True)],
                "pk": ["id"],
            },
            orders={
                "columns": [
                    ("id", "integer", False, True),
                    ("user_id", "integer", True, False),
                ],
                "pk": ["id"],
                "fks": [("user_id", "users", "id")],
            },
        )
        config = ConfigManager.generate_default_config(s)

        (tmp_path / "users.csv").write_text("id\n1\n")
        (tmp_path / "orders.csv").write_text("id,user_id\n10,1\n20,\n")

        report = validate_data(s, config, tmp_path)
        assert report.is_clean


class TestValidateDataMissingDumps:
    def test_missing_source_dump_skipped(self, tmp_path):
        s = _schema(
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
        config = ConfigManager.generate_default_config(s)
        (tmp_path / "users.csv").write_text("id\n1\n")
        # orders.csv intentionally missing

        report = validate_data(s, config, tmp_path)
        assert report.is_clean
        assert report.tables_checked == 0

    def test_missing_target_dump_skipped(self, tmp_path):
        s = _schema(
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
        config = ConfigManager.generate_default_config(s)
        # users.csv intentionally missing
        (tmp_path / "orders.csv").write_text("id,user_id\n10,1\n")

        report = validate_data(s, config, tmp_path)
        assert report.is_clean


class TestValidateDataSummary:
    def test_summary_by_fk(self, tmp_path):
        s = _schema(
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
        config = ConfigManager.generate_default_config(s)

        (tmp_path / "users.csv").write_text("id\n1\n")
        (tmp_path / "orders.csv").write_text("id,user_id\n10,1\n20,5\n30,6\n")

        report = validate_data(s, config, tmp_path)
        summary = report.summary_by_fk()
        assert "orders.user_id -> users" in summary
        assert summary["orders.user_id -> users"] == 2
