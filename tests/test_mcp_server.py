from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from r2g.catalog import CatalogManager
from r2g.config import ConfigManager

MCP_MODULE = "r2g.mcp_server"


@pytest.fixture
def catalog(tmp_path):
    return CatalogManager(str(tmp_path))


@pytest.fixture
def seeded_catalog(catalog, sample_schema):
    catalog.add_source("test_pg", "postgresql", "postgresql://test@localhost/testdb")
    catalog.create_snapshot("test_pg", sample_schema)
    return catalog


@pytest.fixture
def project_catalog(seeded_catalog, sample_schema, tmp_path):
    config = ConfigManager.generate_default_config(sample_schema)
    mapping_path = str(tmp_path / "mapping.yaml")
    ConfigManager.save_config(config, mapping_path)
    seeded_catalog.create_project("demo", "test_pg", mapping_path)
    return seeded_catalog


def _patch_catalog(catalog):
    return patch(f"{MCP_MODULE}._get_catalog", return_value=catalog)


class TestListSources:
    def test_empty(self, catalog):
        from r2g.mcp_server import list_sources

        with _patch_catalog(catalog):
            result = list_sources()
        assert result == []

    def test_with_sources(self, seeded_catalog):
        from r2g.mcp_server import list_sources

        with _patch_catalog(seeded_catalog):
            result = list_sources()
        assert len(result) == 1
        assert result[0]["name"] == "test_pg"


class TestGetSource:
    def test_found(self, seeded_catalog):
        from r2g.mcp_server import get_source

        with _patch_catalog(seeded_catalog):
            result = get_source("test_pg")
        assert result["name"] == "test_pg"
        assert result["source_type"] == "postgresql"

    def test_not_found(self, catalog):
        from r2g.mcp_server import get_source

        with _patch_catalog(catalog):
            result = get_source("nope")
        assert "error" in result


class TestAddSource:
    def test_add(self, catalog):
        from r2g.mcp_server import add_source

        with _patch_catalog(catalog):
            result = add_source("new_src", "postgresql://x", description="test")
        assert result["status"] == "created"
        assert result["source"]["name"] == "new_src"

    def test_duplicate(self, seeded_catalog):
        from r2g.mcp_server import add_source

        with _patch_catalog(seeded_catalog):
            result = add_source("test_pg", "postgresql://x")
        assert "error" in result


class TestRemoveSource:
    def test_remove(self, seeded_catalog):
        from r2g.mcp_server import remove_source

        with _patch_catalog(seeded_catalog):
            result = remove_source("test_pg", cascade=True)
        assert result["status"] == "removed"

    def test_not_found(self, catalog):
        from r2g.mcp_server import remove_source

        with _patch_catalog(catalog):
            result = remove_source("nope")
        assert "error" in result


class TestListProjects:
    def test_empty(self, catalog):
        from r2g.mcp_server import list_projects

        with _patch_catalog(catalog):
            result = list_projects()
        assert result == []

    def test_with_projects(self, project_catalog):
        from r2g.mcp_server import list_projects

        with _patch_catalog(project_catalog):
            result = list_projects()
        assert len(result) == 1
        assert result[0]["name"] == "demo"


class TestGetProject:
    def test_found(self, project_catalog):
        from r2g.mcp_server import get_project

        with _patch_catalog(project_catalog):
            result = get_project("demo")
        assert result["name"] == "demo"
        assert result["source_name"] == "test_pg"

    def test_not_found(self, catalog):
        from r2g.mcp_server import get_project

        with _patch_catalog(catalog):
            result = get_project("nope")
        assert "error" in result


class TestCreateProject:
    def test_create(self, seeded_catalog, tmp_path, sample_schema):
        from r2g.mcp_server import create_project

        config = ConfigManager.generate_default_config(sample_schema)
        mapping_path = str(tmp_path / "new_mapping.yaml")
        ConfigManager.save_config(config, mapping_path)

        with _patch_catalog(seeded_catalog):
            result = create_project("new_proj", "test_pg", mapping_path)
        assert result["status"] == "created"

    def test_bad_source(self, catalog):
        from r2g.mcp_server import create_project

        with _patch_catalog(catalog):
            result = create_project("p", "nonexistent", "/dev/null")
        assert "error" in result


class TestTargets:
    def test_list_empty(self, catalog):
        from r2g.mcp_server import list_targets

        with _patch_catalog(catalog):
            result = list_targets()
        assert result == []

    def test_add_and_list(self, catalog):
        from r2g.mcp_server import add_target, list_targets

        with _patch_catalog(catalog):
            result = add_target("local", description="test arango")
            assert result["status"] == "created"

            targets = list_targets()
            assert len(targets) == 1
            assert targets[0]["name"] == "local"


class TestGenerateMapping:
    def test_generate(self, seeded_catalog):
        from r2g.mcp_server import generate_mapping

        with _patch_catalog(seeded_catalog):
            result = generate_mapping("test_pg")
        assert result["collections"] == 2
        assert result["edges"] == 1
        assert "mapping" in result

    def test_no_snapshot(self, catalog):
        from r2g.mcp_server import generate_mapping

        catalog.add_source("bare", "postgresql", "postgresql://x")
        with _patch_catalog(catalog):
            result = generate_mapping("bare")
        assert "error" in result

    def test_save_to_file(self, seeded_catalog, tmp_path):
        from r2g.mcp_server import generate_mapping

        save_path = str(tmp_path / "out.yaml")
        with _patch_catalog(seeded_catalog):
            result = generate_mapping("test_pg", save_path=save_path)
        assert result["saved_to"] == save_path
        config = ConfigManager.load_config(save_path)
        assert len(config.collections) == 2


class TestValidateMapping:
    def test_valid(self, project_catalog):
        from r2g.mcp_server import validate_mapping

        project = project_catalog.get_project("demo")
        with _patch_catalog(project_catalog):
            result = validate_mapping("test_pg", project.mapping_config_path)
        assert result["valid"] is True
        assert result["issues"] == []

    def test_no_snapshot(self, catalog):
        from r2g.mcp_server import validate_mapping

        catalog.add_source("bare", "postgresql", "postgresql://x")
        with _patch_catalog(catalog):
            result = validate_mapping("bare", "/nonexistent.yaml")
        assert "error" in result


class TestDiffSchemaSnapshots:
    def test_not_enough_snapshots(self, seeded_catalog):
        from r2g.mcp_server import diff_schema_snapshots

        with _patch_catalog(seeded_catalog):
            result = diff_schema_snapshots("test_pg")
        assert "error" in result

    def test_with_two_snapshots(self, seeded_catalog, sample_schema):
        from r2g.mcp_server import diff_schema_snapshots

        seeded_catalog.create_snapshot("test_pg", sample_schema)
        with _patch_catalog(seeded_catalog):
            result = diff_schema_snapshots("test_pg")
        assert "added_tables" in result
        assert "removed_tables" in result


class TestLoadHistory:
    def test_empty(self, catalog):
        from r2g.mcp_server import load_history

        with _patch_catalog(catalog):
            result = load_history()
        assert result == []

    def test_with_records(self, project_catalog):
        from r2g.mcp_server import load_history

        project_catalog.start_load("demo", "streaming")
        with _patch_catalog(project_catalog):
            result = load_history("demo")
        assert len(result) == 1
        assert result[0]["project_name"] == "demo"


class TestDiffMappings:
    def test_no_changes(self, project_catalog):
        from r2g.mcp_server import diff_mappings

        project = project_catalog.get_project("demo")
        path = project.mapping_config_path
        with _patch_catalog(project_catalog):
            result = diff_mappings(path, path, "test_pg")
        assert result["changes"] == []
        assert result["actions"] == []


class TestResources:
    def test_sources_resource(self, seeded_catalog):
        from r2g.mcp_server import resource_sources

        with _patch_catalog(seeded_catalog):
            raw = resource_sources()
        data = json.loads(raw)
        assert len(data) == 1

    def test_projects_resource(self, project_catalog):
        from r2g.mcp_server import resource_projects

        with _patch_catalog(project_catalog):
            raw = resource_projects()
        data = json.loads(raw)
        assert len(data) == 1

    def test_schema_resource(self, seeded_catalog):
        from r2g.mcp_server import resource_schema

        with _patch_catalog(seeded_catalog):
            raw = resource_schema("test_pg")
        data = json.loads(raw)
        assert data["table_count"] == 2
        assert "users" in data["tables"]

    def test_schema_resource_missing(self, catalog):
        from r2g.mcp_server import resource_schema

        with _patch_catalog(catalog):
            raw = resource_schema("nope")
        data = json.loads(raw)
        assert "error" in data

    def test_mapping_resource(self, project_catalog):
        from r2g.mcp_server import resource_mapping

        with _patch_catalog(project_catalog):
            raw = resource_mapping("demo")
        data = json.loads(raw)
        assert "collections" in data
        assert "edges" in data

    def test_history_resource(self, project_catalog):
        from r2g.mcp_server import resource_history

        with _patch_catalog(project_catalog):
            raw = resource_history("demo")
        data = json.loads(raw)
        assert isinstance(data, list)

    def test_targets_resource(self, catalog):
        from r2g.mcp_server import resource_targets

        with _patch_catalog(catalog):
            raw = resource_targets()
        data = json.loads(raw)
        assert data == []
