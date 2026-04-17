from __future__ import annotations

import pytest

from r2g.catalog import CatalogManager, DependencyError


class TestUpdateSource:
    def test_update_source_happy_path(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "postgresql://u:p@host/db", description="original")
        updated = mgr.update_source("pg1", description="updated desc", owner="bob")
        assert updated.description == "updated desc"
        assert updated.owner == "bob"
        assert updated.name == "pg1"
        assert updated.updated_at > updated.created_at

    def test_update_source_persists(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.update_source("pg1", owner="alice")
        reloaded = CatalogManager(catalog_dir=tmp_path).get_source("pg1")
        assert reloaded.owner == "alice"

    def test_update_source_nonexistent_raises(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        with pytest.raises(ValueError, match="not found"):
            mgr.update_source("ghost", description="nope")


class TestCascadingDelete:
    def _setup_source_with_deps(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_snapshot("pg1", sample_schema)
        mgr.create_snapshot("pg1", sample_schema)
        mgr.create_project("proj_a", "pg1", "a.yaml")
        mgr.create_project("proj_b", "pg1", "b.yaml")
        mgr.start_load("proj_a", "full")
        mgr.start_load("proj_a", "streaming")
        mgr.start_load("proj_b", "full")
        return mgr

    def test_remove_source_no_cascade_raises(self, tmp_path, sample_schema):
        mgr = self._setup_source_with_deps(tmp_path, sample_schema)
        with pytest.raises(DependencyError) as exc_info:
            mgr.remove_source("pg1")
        err = exc_info.value
        assert err.source_name == "pg1"
        assert set(err.projects) == {"proj_a", "proj_b"}
        assert len(err.snapshots) == 2
        assert err.load_records == 3

    def test_remove_source_cascade_deletes_everything(self, tmp_path, sample_schema):
        mgr = self._setup_source_with_deps(tmp_path, sample_schema)
        result = mgr.remove_source("pg1", cascade=True)
        assert result is True
        assert mgr.get_source("pg1") is None
        assert mgr.list_projects() == []
        assert mgr.list_snapshots("pg1") == []
        assert mgr.get_history() == []

    def test_remove_source_no_deps_no_cascade_succeeds(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        assert mgr.remove_source("pg1") is True
        assert mgr.get_source("pg1") is None

    def test_remove_source_cascade_preserves_unrelated(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn1")
        mgr.add_source("pg2", "postgresql", "conn2")
        mgr.create_snapshot("pg1", sample_schema)
        mgr.create_snapshot("pg2", sample_schema)
        mgr.create_project("proj1", "pg1", "a.yaml")
        mgr.create_project("proj2", "pg2", "b.yaml")
        mgr.start_load("proj1", "full")
        mgr.start_load("proj2", "full")

        mgr.remove_source("pg1", cascade=True)

        assert mgr.get_source("pg2") is not None
        assert len(mgr.list_projects()) == 1
        assert mgr.list_projects()[0].name == "proj2"
        assert len(mgr.list_snapshots("pg2")) == 1
        assert len(mgr.get_history(project_name="proj2")) == 1

    def test_remove_nonexistent_returns_false(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.remove_source("nope", cascade=True) is False

    def test_dependency_error_message(self, tmp_path, sample_schema):
        mgr = self._setup_source_with_deps(tmp_path, sample_schema)
        with pytest.raises(DependencyError, match="Cannot remove source 'pg1'"):
            mgr.remove_source("pg1")


class TestTargetCRUD:
    def test_add_target(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        t = mgr.add_target("arango_prod", endpoint="http://prod:8529", database="mydb", description="production")
        assert t.name == "arango_prod"
        assert t.endpoint == "http://prod:8529"
        assert t.database == "mydb"
        assert t.description == "production"
        assert t.username == "root"
        assert t.created_at is not None

    def test_add_target_duplicate_raises(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_target("t1")
        with pytest.raises(ValueError, match="already exists"):
            mgr.add_target("t1")

    def test_list_targets(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_target("t1")
        mgr.add_target("t2", endpoint="http://other:8529")
        targets = mgr.list_targets()
        assert len(targets) == 2
        assert {t.name for t in targets} == {"t1", "t2"}

    def test_get_target(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_target("t1", database="graphdb")
        t = mgr.get_target("t1")
        assert t is not None
        assert t.database == "graphdb"

    def test_get_target_missing(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.get_target("nope") is None

    def test_update_target(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_target("t1", database="old_db")
        updated = mgr.update_target("t1", database="new_db", description="changed")
        assert updated.database == "new_db"
        assert updated.description == "changed"
        assert updated.updated_at > updated.created_at

    def test_update_target_nonexistent_raises(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        with pytest.raises(ValueError, match="not found"):
            mgr.update_target("ghost", database="x")

    def test_remove_target(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_target("t1")
        assert mgr.remove_target("t1") is True
        assert mgr.get_target("t1") is None

    def test_remove_target_missing(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.remove_target("nope") is False

    def test_target_persistence(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_target("t1", endpoint="http://a:8529", database="db1", username="admin", password="s3cret")
        mgr2 = CatalogManager(catalog_dir=tmp_path)
        t = mgr2.get_target("t1")
        assert t.endpoint == "http://a:8529"
        assert t.password == "s3cret"


class TestSourceParams:
    def test_csv_source_params(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        src = mgr.add_source(
            "csv1",
            "csv",
            "/data/csvs",
            source_params={"directory": "/data/csvs", "delimiter": ","},
        )
        assert src.source_type == "csv"
        assert src.source_params["directory"] == "/data/csvs"
        assert src.source_params["delimiter"] == ","

    def test_kafka_source_params(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        src = mgr.add_source(
            "kafka1",
            "kafka",
            "kafka://localhost:9092",
            source_params={
                "bootstrap_servers": "localhost:9092",
                "topic": "events",
                "schema_registry_url": "http://registry:8081",
            },
        )
        assert src.source_type == "kafka"
        assert src.source_params["bootstrap_servers"] == "localhost:9092"
        assert src.source_params["topic"] == "events"

    def test_source_params_default_empty(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        src = mgr.add_source("pg1", "postgresql", "conn")
        assert src.source_params == {}

    def test_source_params_persists(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("csv1", "csv", "/data", source_params={"delimiter": ";"})
        mgr2 = CatalogManager(catalog_dir=tmp_path)
        src = mgr2.get_source("csv1")
        assert src.source_params == {"delimiter": ";"}

    def test_update_source_params(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("csv1", "csv", "/data", source_params={"delimiter": ","})
        updated = mgr.update_source("csv1", source_params={"delimiter": ";", "encoding": "utf-8"})
        assert updated.source_params == {"delimiter": ";", "encoding": "utf-8"}
