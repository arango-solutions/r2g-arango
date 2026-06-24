from __future__ import annotations

import json
from pathlib import Path
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

    def test_save_to_file(self, seeded_catalog):
        from r2g.mcp_server import generate_mapping

        # A relative save_path is resolved inside the catalog projects jail.
        with _patch_catalog(seeded_catalog):
            result = generate_mapping("test_pg", save_path="demo/out.yaml")
        saved_to = result["saved_to"]
        assert saved_to is not None
        assert (seeded_catalog.dir / "projects") in Path(saved_to).parents
        config = ConfigManager.load_config(saved_to)
        assert len(config.collections) == 2

    def test_save_path_outside_jail_rejected(self, seeded_catalog, tmp_path):
        from r2g.mcp_server import generate_mapping

        outside = str(tmp_path / "escape.yaml")
        with _patch_catalog(seeded_catalog):
            result = generate_mapping("test_pg", save_path=outside)
        assert "error" in result
        assert "projects directory" in result["error"]
        assert not Path(outside).exists()

    def test_save_path_traversal_rejected(self, seeded_catalog):
        from r2g.mcp_server import generate_mapping

        with _patch_catalog(seeded_catalog):
            result = generate_mapping("test_pg", save_path="../../etc/evil.yaml")
        assert "error" in result


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


class _FakeProvider:
    """A stand-in catalog provider with canned discovery responses."""

    def __init__(self):
        from r2g.catalogs.base import ASSET_DATABASE, ASSET_TABLE, CatalogAsset

        self._db = CatalogAsset(
            provider="corp",
            provider_type="openmetadata",
            fqn="pg.shop",
            kind=ASSET_DATABASE,
            name="shop",
            source_type="postgresql",
            connection_hint={"hostPort": "db.internal:5432", "database": "shop"},
        )
        self._table = CatalogAsset(
            provider="corp",
            provider_type="openmetadata",
            fqn="pg.shop.public.orders",
            kind=ASSET_TABLE,
            name="orders",
            source_type="postgresql",
        )

    def list_data_sources(self):
        return [self._db]

    def list_children(self, asset):
        return [self._table]

    def search(self, query, *, limit=50):
        return [self._table]

    def get_asset(self, fqn):
        if fqn == self._db.fqn:
            return self._db
        if fqn == self._table.fqn:
            return self._table
        return None

    def resolve_source(self, asset):
        from r2g.catalogs.base import ResolvedSource

        return ResolvedSource(
            source_type="postgresql",
            connection_string="postgresql://$R2G_DB_USER:$R2G_DB_PASSWORD@db.internal:5432/shop",
            schema_name="public",
            notes="Credentials are not read from the catalog.",
        )


class TestCatalogRegistry:
    def test_list_empty(self, catalog):
        from r2g.mcp_server import list_catalogs

        with _patch_catalog(catalog):
            assert list_catalogs() == []

    def test_add_list_redacts_token(self, catalog):
        from r2g.mcp_server import add_catalog, list_catalogs

        with _patch_catalog(catalog):
            result = add_catalog("corp", "http://localhost:8585", token="super-secret-token")
            assert result["status"] == "created"
            # Token must never be echoed back in the clear.
            assert "super-secret-token" not in json.dumps(result)
            listed = list_catalogs()
        assert len(listed) == 1
        assert listed[0]["name"] == "corp"
        assert "super-secret-token" not in json.dumps(listed)

    def test_add_unsupported_type(self, catalog):
        from r2g.mcp_server import add_catalog

        with _patch_catalog(catalog):
            result = add_catalog("bad", "http://x", provider_type="nope")
        assert "error" in result

    def test_remove(self, catalog):
        from r2g.mcp_server import add_catalog, remove_catalog

        with _patch_catalog(catalog):
            add_catalog("corp", "http://localhost:8585")
            assert remove_catalog("corp")["status"] == "removed"
            assert "error" in remove_catalog("corp")


class TestCatalogBrowse:
    def test_unknown_catalog(self, catalog):
        from r2g.mcp_server import catalog_browse

        with _patch_catalog(catalog):
            result = catalog_browse("ghost")
        assert "error" in result

    def test_list_sources(self, catalog):
        import r2g.mcp_server as m

        with _patch_catalog(catalog), patch.object(
            m, "_build_catalog_provider", return_value=_FakeProvider()
        ):
            result = m.catalog_browse("corp")
        assert result["count"] == 1
        assert result["assets"][0]["name"] == "shop"

    def test_descend_path(self, catalog):
        import r2g.mcp_server as m

        with _patch_catalog(catalog), patch.object(
            m, "_build_catalog_provider", return_value=_FakeProvider()
        ):
            result = m.catalog_browse("corp", path="pg.shop")
        assert result["assets"][0]["name"] == "orders"

    def test_path_not_found(self, catalog):
        import r2g.mcp_server as m

        with _patch_catalog(catalog), patch.object(
            m, "_build_catalog_provider", return_value=_FakeProvider()
        ):
            result = m.catalog_browse("corp", path="pg.nope")
        assert "error" in result

    def test_search(self, catalog):
        import r2g.mcp_server as m

        with _patch_catalog(catalog), patch.object(
            m, "_build_catalog_provider", return_value=_FakeProvider()
        ):
            result = m.catalog_browse("corp", search="ord")
        assert result["count"] == 1


class TestCatalogImportSource:
    def test_import_creates_source(self, catalog):
        import r2g.mcp_server as m

        with _patch_catalog(catalog), patch.object(
            m, "_build_catalog_provider", return_value=_FakeProvider()
        ):
            result = m.catalog_import_source("corp", "pg.shop", "shop_src")
        assert result["status"] == "imported"
        assert result["source"]["name"] == "shop_src"
        assert result["source_type"] == "postgresql"
        assert result["schema_name"] == "public"
        # The created source actually lands in the catalog, redacted.
        src = catalog.get_source("shop_src")
        assert src is not None
        assert src.source_type == "postgresql"
        # Connection string (with $ENV placeholders) is redacted in the response.
        assert "$R2G_DB_PASSWORD" not in json.dumps(result["source"])

    def test_asset_not_found(self, catalog):
        import r2g.mcp_server as m

        with _patch_catalog(catalog), patch.object(
            m, "_build_catalog_provider", return_value=_FakeProvider()
        ):
            result = m.catalog_import_source("corp", "pg.ghost", "x")
        assert "error" in result


class TestSafeError:
    def test_scrubs_dsn_credentials(self):
        from r2g.mcp_server import _safe_error

        exc = RuntimeError(
            "connection failed: postgresql://admin:s3cret@db.internal:5432/prod"
        )
        msg = _safe_error(exc)
        assert "s3cret" not in msg
        assert "admin" not in msg

    def test_passes_through_plain_message(self):
        from r2g.mcp_server import _safe_error

        assert _safe_error(ValueError("no snapshot found")) == "no snapshot found"

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
