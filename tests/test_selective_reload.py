from __future__ import annotations

import sys
from unittest.mock import MagicMock

import click
import pytest
from typer.testing import CliRunner

from r2g.main import app
from r2g.mapping_diff import ReloadAction, ReloadPlan
from r2g.selective_reload import ReloadReport, SelectiveReloader

runner = CliRunner()


def plain_output(result) -> str:
    """Strip Rich/Typer ANSI styling so help assertions are version-stable."""
    return click.unstyle(result.output)


@pytest.fixture(autouse=True)
def _reset_structlog():
    yield
    import structlog

    structlog.reset_defaults()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


def _mock_writer() -> MagicMock:
    writer = MagicMock()
    writer.drop_collection.return_value = True
    writer.ensure_collection.return_value = None
    writer.db.collection.return_value.rename.return_value = None
    writer.db.aql.execute.return_value = None
    return writer


class TestSelectiveReloader:
    def test_dry_run_skips_all_actions(self):
        plan = ReloadPlan(actions=[
            ReloadAction(action_type="drop_collection", collection="col1", reason="removed"),
            ReloadAction(action_type="drop_edge", collection="edge1", reason="removed"),
        ])
        reloader = SelectiveReloader(writer=_mock_writer(), plan=plan)
        report = reloader.execute(dry_run=True)

        assert len(report.actions_skipped) == 2
        assert len(report.actions_executed) == 0
        assert len(report.errors) == 0

    def test_drop_collection_dispatches(self):
        writer = _mock_writer()
        plan = ReloadPlan(actions=[
            ReloadAction(action_type="drop_collection", collection="old_coll", reason="removed"),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan)
        report = reloader.execute()

        writer.drop_collection.assert_called_once_with("old_coll")
        assert len(report.actions_executed) == 1
        assert report.actions_executed[0]["action"] == "drop_collection"

    def test_drop_edge_dispatches(self):
        writer = _mock_writer()
        plan = ReloadPlan(actions=[
            ReloadAction(action_type="drop_edge", collection="edge_coll", reason="removed edge"),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan)
        report = reloader.execute()

        writer.drop_collection.assert_called_once_with("edge_coll")
        assert len(report.actions_executed) == 1
        assert report.actions_executed[0]["action"] == "drop_edge"

    def test_rename_collection_calls_rename(self):
        writer = _mock_writer()
        # old exists, new does not (so the idempotency guard lets the rename run)
        writer.db.has_collection.side_effect = lambda n: n == "old_name"
        plan = ReloadPlan(actions=[
            ReloadAction(
                action_type="rename_collection",
                collection="old_name",
                reason="renamed to 'new_name'",
                params={"old_name": "old_name", "new_name": "new_name"},
            ),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan)
        report = reloader.execute()

        writer.db.collection.assert_called_with("old_name")
        writer.db.collection("old_name").rename.assert_called_with("new_name")
        assert len(report.actions_executed) == 1
        assert "old_name -> new_name" in report.actions_executed[0]["collection"]

    def test_aql_update_calls_execute(self):
        writer = _mock_writer()
        aql = "FOR doc IN @@coll UPDATE doc WITH {x: 1} IN @@coll"
        plan = ReloadPlan(actions=[
            ReloadAction(
                action_type="aql_update",
                collection="my_coll",
                reason="update fields",
                aql_query=aql,
            ),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan)
        report = reloader.execute()

        writer.db.aql.execute.assert_called_once_with(
            aql,
            bind_vars={"@edge_collection": "my_coll", "@coll": "my_coll"},
        )
        assert len(report.actions_executed) == 1
        assert report.actions_executed[0]["action"] == "aql_update"

    def test_aql_update_binds_params(self):
        writer = _mock_writer()
        plan = ReloadPlan(actions=[
            ReloadAction(
                action_type="aql_update",
                collection="users",
                reason="rename property",
                aql_query="FOR doc IN @@coll REPLACE doc WITH MERGE(UNSET(doc,@old_name),{@new_name: doc.@old_name}) IN @@coll",
                params={"old_name": "first_name", "new_name": "firstName"},
            ),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan)
        reloader.execute()

        _, kwargs = writer.db.aql.execute.call_args
        assert kwargs["bind_vars"]["old_name"] == "first_name"
        assert kwargs["bind_vars"]["new_name"] == "firstName"
        assert kwargs["bind_vars"]["@coll"] == "users"

    def test_rebuild_graph_recreates_named_graph(self):
        from r2g.types import CollectionMapping, EdgeDefinition, MappingConfig

        writer = _mock_writer()
        config = MappingConfig(
            collections={
                "users": CollectionMapping(source_table="users", target_collection="Users"),
                "orders": CollectionMapping(source_table="orders", target_collection="Orders"),
            },
            edges=[EdgeDefinition(
                edge_collection="userOrders", from_collection="users",
                to_collection="orders", from_field="id", to_field="user_id",
            )],
        )
        plan = ReloadPlan(actions=[
            ReloadAction(action_type="rebuild_graph", collection="", reason="after rename"),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan, config=config, graph_name="g1")
        report = reloader.execute()

        writer.create_named_graph.assert_called_once()
        gname, defs = writer.create_named_graph.call_args[0]
        assert gname == "g1"
        # vertex collections resolved to target names
        assert defs[0]["from_vertex_collections"] == ["Users"]
        assert defs[0]["to_vertex_collections"] == ["Orders"]
        assert len(report.actions_executed) == 1

    def test_rebuild_graph_without_name_skips(self):
        writer = _mock_writer()
        plan = ReloadPlan(actions=[
            ReloadAction(action_type="rebuild_graph", collection="", reason="x"),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan, graph_name=None)
        report = reloader.execute()
        writer.create_named_graph.assert_not_called()
        assert len(report.actions_skipped) == 1

    def test_rename_collection_skips_when_target_exists(self):
        writer = _mock_writer()
        writer.db.has_collection.return_value = True  # new already exists
        plan = ReloadPlan(actions=[
            ReloadAction(
                action_type="rename_collection", collection="old", reason="renamed to 'new'",
                params={"old_name": "old", "new_name": "new"},
            ),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan)
        report = reloader.execute()
        writer.db.collection("old").rename.assert_not_called()
        assert len(report.actions_skipped) == 1

    def test_aql_update_without_query_skips(self):
        writer = _mock_writer()
        plan = ReloadPlan(actions=[
            ReloadAction(
                action_type="aql_update",
                collection="my_coll",
                reason="update fields",
                aql_query=None,
            ),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan)
        report = reloader.execute()

        writer.db.aql.execute.assert_not_called()
        assert len(report.actions_skipped) == 1

    def test_unknown_action_type_handled(self):
        writer = _mock_writer()
        plan = ReloadPlan(actions=[
            ReloadAction(action_type="teleport", collection="x", reason="magic"),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan)
        report = reloader.execute()

        assert len(report.actions_executed) == 0
        assert len(report.errors) == 0
        assert len(report.actions_skipped) == 0

    def test_error_captured_in_report(self):
        writer = _mock_writer()
        writer.drop_collection.side_effect = RuntimeError("boom")
        plan = ReloadPlan(actions=[
            ReloadAction(action_type="drop_collection", collection="fail_coll", reason="test"),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan)
        report = reloader.execute()

        assert len(report.errors) == 1
        assert report.errors[0]["error"] == "boom"
        assert report.errors[0]["collection"] == "fail_coll"

    def test_reload_collection_without_pg_conn_skips(self):
        writer = _mock_writer()
        plan = ReloadPlan(actions=[
            ReloadAction(
                action_type="reload_collection",
                collection="coll1",
                reason="new collection",
            ),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan, pg_conn_string=None)
        report = reloader.execute()

        assert len(report.actions_skipped) == 1
        assert report.actions_skipped[0]["reason"] == "no PostgreSQL connection configured"

    def test_reload_edge_without_pg_conn_skips(self):
        writer = _mock_writer()
        plan = ReloadPlan(actions=[
            ReloadAction(
                action_type="reload_edge",
                collection="edge1",
                reason="new edge",
            ),
        ])
        reloader = SelectiveReloader(writer=writer, plan=plan, pg_conn_string=None)
        report = reloader.execute()

        assert len(report.actions_skipped) == 1
        assert report.actions_skipped[0]["reason"] == "no PostgreSQL connection configured"

    def test_empty_plan_produces_empty_report(self):
        plan = ReloadPlan()
        reloader = SelectiveReloader(writer=_mock_writer(), plan=plan)
        report = reloader.execute()

        assert report == ReloadReport()
        assert report.rows_reloaded == 0


class TestMappingDiffCLI:
    def test_mapping_diff_help(self):
        result = runner.invoke(app, ["mapping-diff", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--schema" in output
        assert "--json" in output
        assert "OLD_CONFIG" in output
        assert "NEW_CONFIG" in output

    def test_selective_reload_help(self):
        result = runner.invoke(app, ["selective-reload", "--help"])
        assert result.exit_code == 0
        output = plain_output(result)
        assert "--schema" in output
        assert "--dry-run" in output
        assert "--pg-conn" in output
        assert "--endpoint" in output
        assert "--batch-size" in output

    def test_mapping_diff_identical(self, tmp_path):
        from r2g.config import ConfigManager
        from r2g.types import Column, Schema, Table

        table = Table(
            name="users",
            columns=[Column(name="id", data_type="integer", is_primary_key=True)],
            primary_key=["id"],
            foreign_keys=[],
        )
        schema = Schema(tables={"users": table})
        schema_path = tmp_path / "schema.json"
        schema.save_to_file(str(schema_path))

        config = ConfigManager.generate_default_config(schema)
        cfg_path = tmp_path / "mapping.yaml"
        ConfigManager.save_config(config, cfg_path)

        result = runner.invoke(app, [
            "mapping-diff",
            str(cfg_path), str(cfg_path),
            "--schema", str(schema_path),
        ])
        assert result.exit_code == 0
        assert "identical" in result.output.lower()

    def test_mapping_diff_json_output(self, tmp_path):
        import json

        from r2g.config import ConfigManager
        from r2g.types import Column, ForeignKey, Schema, Table

        users = Table(
            name="users",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="name", data_type="text"),
            ],
            primary_key=["id"],
            foreign_keys=[],
        )
        orders = Table(
            name="orders",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="user_id", data_type="integer"),
            ],
            primary_key=["id"],
            foreign_keys=[ForeignKey(column="user_id", foreign_table="users", foreign_column="id")],
        )
        schema = Schema(tables={"users": users, "orders": orders})
        schema_path = tmp_path / "schema.json"
        schema.save_to_file(str(schema_path))

        old_config = ConfigManager.generate_default_config(schema)
        old_path = tmp_path / "old.yaml"
        ConfigManager.save_config(old_config, old_path)

        new_config = ConfigManager.generate_default_config(schema)
        new_config.key_separator = "::"
        new_path = tmp_path / "new.yaml"
        ConfigManager.save_config(new_config, new_path)

        result = runner.invoke(app, [
            "mapping-diff",
            str(old_path), str(new_path),
            "--schema", str(schema_path),
            "--json",
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "changes" in data
        assert len(data["changes"]) > 0
