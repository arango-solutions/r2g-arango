from __future__ import annotations

import json

import pytest

from r2g.catalog import CatalogManager


class TestSourceCRUD:
    def test_add_source(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        src = mgr.add_source("pg1", "postgresql", "postgresql://u:p@host/db", description="test db", owner="alice")
        assert src.name == "pg1"
        assert src.source_type == "postgresql"
        assert src.description == "test db"
        assert src.owner == "alice"
        assert src.created_at is not None

    def test_list_sources(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("a", "postgresql", "conn_a")
        mgr.add_source("b", "postgresql", "conn_b")
        sources = mgr.list_sources()
        names = {s.name for s in sources}
        assert names == {"a", "b"}

    def test_get_source(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        assert mgr.get_source("pg1") is not None
        assert mgr.get_source("pg1").name == "pg1"

    def test_get_source_missing(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.get_source("nonexistent") is None

    def test_remove_source(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        assert mgr.remove_source("pg1") is True
        assert mgr.get_source("pg1") is None
        assert mgr.list_sources() == []

    def test_remove_nonexistent_source(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.remove_source("ghost") is False

    def test_duplicate_source_name_raises(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        with pytest.raises(ValueError, match="already exists"):
            mgr.add_source("pg1", "postgresql", "conn2")


class TestSnapshots:
    def test_create_snapshot(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        snap = mgr.create_snapshot("pg1", sample_schema, pg_schema="public")
        assert snap.source_name == "pg1"
        assert snap.pg_schema == "public"
        assert len(snap.schema_data.tables) == 2
        assert snap.id

    def test_get_latest_snapshot(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_snapshot("pg1", sample_schema)
        snap2 = mgr.create_snapshot("pg1", sample_schema)
        latest = mgr.get_latest_snapshot("pg1")
        assert latest is not None
        assert latest.id == snap2.id

    def test_get_latest_snapshot_no_snapshots(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.get_latest_snapshot("pg1") is None

    def test_list_snapshots(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_snapshot("pg1", sample_schema)
        mgr.create_snapshot("pg1", sample_schema)
        snaps = mgr.list_snapshots("pg1")
        assert len(snaps) == 2
        assert snaps[0].captured_at <= snaps[1].captured_at

    def test_list_snapshots_filters_by_source(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn1")
        mgr.add_source("pg2", "postgresql", "conn2")
        mgr.create_snapshot("pg1", sample_schema)
        mgr.create_snapshot("pg2", sample_schema)
        assert len(mgr.list_snapshots("pg1")) == 1
        assert len(mgr.list_snapshots("pg2")) == 1


class TestProjectCRUD:
    def test_create_project(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_snapshot("pg1", sample_schema)
        proj = mgr.create_project("my_project", "pg1", "mapping.yaml")
        assert proj.name == "my_project"
        assert proj.source_name == "pg1"
        assert proj.schema_snapshot_id != ""
        assert proj.mapping_config_path == "mapping.yaml"

    def test_create_project_missing_source_raises(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        with pytest.raises(ValueError, match="not found"):
            mgr.create_project("proj", "no_source", "mapping.yaml")

    def test_create_project_no_snapshot(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        proj = mgr.create_project("proj", "pg1", "mapping.yaml")
        assert proj.schema_snapshot_id == ""

    def test_list_projects(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_project("proj_a", "pg1", "a.yaml")
        mgr.create_project("proj_b", "pg1", "b.yaml")
        projects = mgr.list_projects()
        names = {p.name for p in projects}
        assert names == {"proj_a", "proj_b"}

    def test_get_project(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        mgr.create_project("proj", "pg1", "mapping.yaml")
        proj = mgr.get_project("proj")
        assert proj is not None
        assert proj.name == "proj"

    def test_get_project_missing(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.get_project("missing") is None

    def test_create_project_custom_arango_settings(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        proj = mgr.create_project(
            "proj", "pg1", "mapping.yaml",
            arango_endpoint="http://arango:8529",
            arango_database="mydb",
        )
        assert proj.arango_endpoint == "http://arango:8529"
        assert proj.arango_database == "mydb"


class TestLoadHistory:
    def test_start_load(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        record = mgr.start_load("proj", "full", mapping_hash="abc123")
        assert record.project_name == "proj"
        assert record.load_type == "full"
        assert record.status == "running"
        assert record.mapping_hash == "abc123"
        assert record.completed_at is None

    def test_complete_load(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        record = mgr.start_load("proj", "full")
        completed = mgr.complete_load(record.id, rows_loaded=1000, errors=2, collections_loaded=["users", "orders"])
        assert completed.status == "completed"
        assert completed.rows_loaded == 1000
        assert completed.errors == 2
        assert completed.collections_loaded == ["users", "orders"]
        assert completed.completed_at is not None

    def test_complete_load_failed_status(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        record = mgr.start_load("proj", "streaming")
        completed = mgr.complete_load(record.id, rows_loaded=50, errors=10, collections_loaded=[], status="failed")
        assert completed.status == "failed"

    def test_complete_load_not_found_raises(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        with pytest.raises(ValueError, match="not found"):
            mgr.complete_load("nonexistent-id", rows_loaded=0, errors=0, collections_loaded=[])

    def test_get_history_all(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.start_load("proj_a", "full")
        mgr.start_load("proj_b", "streaming")
        history = mgr.get_history()
        assert len(history) == 2

    def test_get_history_by_project(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.start_load("proj_a", "full")
        mgr.start_load("proj_b", "streaming")
        mgr.start_load("proj_a", "cdc")
        history = mgr.get_history(project_name="proj_a")
        assert len(history) == 2
        assert all(r.project_name == "proj_a" for r in history)

    def test_get_history_limit(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        for i in range(10):
            mgr.start_load("proj", "full")
        history = mgr.get_history(limit=3)
        assert len(history) == 3

    def test_get_history_sorted_newest_first(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        r1 = mgr.start_load("proj", "full")
        r2 = mgr.start_load("proj", "streaming")
        history = mgr.get_history()
        assert history[0].id == r2.id
        assert history[1].id == r1.id


class TestCatalogPersistence:
    def test_save_and_load_round_trip(self, tmp_path, sample_schema):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "postgresql://u:p@h/d", description="main", owner="bob")
        mgr.create_snapshot("pg1", sample_schema)
        mgr.create_project("proj", "pg1", "mapping.yaml")
        mgr.start_load("proj", "full")

        mgr2 = CatalogManager(catalog_dir=tmp_path)
        assert len(mgr2.list_sources()) == 1
        assert mgr2.get_source("pg1").owner == "bob"
        assert len(mgr2.list_snapshots("pg1")) == 1
        assert len(mgr2.list_projects()) == 1
        assert len(mgr2.get_history()) == 1

    def test_empty_catalog_on_fresh_dir(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        assert mgr.list_sources() == []
        assert mgr.list_projects() == []
        assert mgr.get_history() == []

    def test_catalog_json_is_valid(self, tmp_path):
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "conn")
        catalog_path = tmp_path / "catalog.json"
        assert catalog_path.exists()
        data = json.loads(catalog_path.read_text(encoding="utf-8"))
        assert "sources" in data
        assert "pg1" in data["sources"]
