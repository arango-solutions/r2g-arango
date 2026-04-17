from __future__ import annotations

import pytest

from r2g.catalog import CatalogManager
from r2g.config import ConfigManager
from r2g.types import Column, MappingConfig, Schema, Table
from r2g.ui.server import create_app


@pytest.fixture
def catalog_dir(tmp_path):
    return str(tmp_path / "catalog")


@pytest.fixture
def client(catalog_dir):
    from starlette.testclient import TestClient

    app = create_app(catalog_dir=catalog_dir)
    return TestClient(app)


class TestHealthEndpoint:
    def test_health(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestSourceEndpoints:
    def test_list_sources_empty(self, client):
        resp = client.get("/api/sources")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_add_source(self, client):
        resp = client.post("/api/sources", json={
            "name": "test_pg",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        assert resp.status_code == 201
        assert resp.json()["name"] == "test_pg"

    def test_add_duplicate_source(self, client):
        client.post("/api/sources", json={
            "name": "test_pg",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        resp = client.post("/api/sources", json={
            "name": "test_pg",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        assert resp.status_code == 409

    def test_remove_source(self, client):
        client.post("/api/sources", json={
            "name": "to_remove",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        resp = client.delete("/api/sources/to_remove")
        assert resp.status_code == 200

    def test_remove_nonexistent_source(self, client):
        resp = client.delete("/api/sources/nonexistent")
        assert resp.status_code == 404


class TestProjectEndpoints:
    def _add_source(self, client):
        client.post("/api/sources", json={
            "name": "src",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })

    def test_list_projects_empty(self, client):
        resp = client.get("/api/projects")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_create_project(self, client, tmp_path):
        self._add_source(client)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(MappingConfig(), mapping_path)
        resp = client.post("/api/projects", json={
            "name": "test_proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })
        assert resp.status_code == 201

    def test_create_project_missing_source(self, client):
        resp = client.post("/api/projects", json={
            "name": "test_proj",
            "source_name": "nonexistent",
            "mapping_config_path": "/fake/path",
        })
        assert resp.status_code == 400


class TestMappingEndpoints:
    def _setup_project(self, client, tmp_path, catalog_dir):
        client.post("/api/sources", json={
            "name": "src",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        config = ConfigManager.generate_default_config(
            Schema(tables={
                "users": Table(name="users", columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="name", data_type="text"),
                ], primary_key=["id"]),
            })
        )
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(config, mapping_path)
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "users": Table(name="users", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="name", data_type="text"),
            ], primary_key=["id"]),
        })
        mgr.create_snapshot("src", schema)
        client.post("/api/projects", json={
            "name": "proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })

    def test_get_mapping(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.get("/api/projects/proj/mapping")
        assert resp.status_code == 200
        assert "collections" in resp.json()

    def test_get_mapping_not_found(self, client):
        resp = client.get("/api/projects/nonexistent/mapping")
        assert resp.status_code == 404

    def test_validate_mapping(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/proj/validate")
        assert resp.status_code == 200
        assert "valid" in resp.json()

    def test_save_mapping(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.put("/api/projects/proj/mapping", json={
            "source_schema": "public",
            "collections": {},
            "edges": [],
            "type_overrides": {},
            "key_separator": "_",
        })
        assert resp.status_code == 200
        assert resp.json()["saved"] is True

    def test_get_graph_data(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.get("/api/projects/proj/graph-data")
        assert resp.status_code == 200
        data = resp.json()
        assert "graph" in data
        assert "tables" in data

    def test_diff_mapping(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/proj/diff", json={
            "source_schema": "public",
            "collections": {},
            "edges": [],
            "type_overrides": {},
            "key_separator": "-",
        })
        assert resp.status_code == 200
        assert "changes" in resp.json()


class TestHistoryEndpoint:
    def test_empty_history(self, client):
        resp = client.get("/api/projects/any/history")
        assert resp.status_code == 200
        assert resp.json() == []


class TestExpressionEndpoints:
    def test_list_functions_advertises_subset(self, client):
        resp = client.get("/api/expressions/functions")
        assert resp.status_code == 200
        body = resp.json()
        assert body["engine"] == "aql"
        assert "CONCAT" in body["functions"]
        assert "UPPER" in body["functions"]
        assert body["bind_syntax"] == "@column_name"

    def test_compile_valid_expression_returns_references(self, client):
        resp = client.post(
            "/api/expressions/compile",
            json={"expression": 'CONCAT(@first, " ", @last)'},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is True
        assert body["references"] == ["first", "last"]

    def test_compile_invalid_expression_returns_error(self, client):
        resp = client.post(
            "/api/expressions/compile",
            json={"expression": "UNKNOWN_FN(@a)"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is False
        assert "UNKNOWN_FN" in body["error"]

    def test_compile_rejects_non_aql_engine(self, client):
        resp = client.post(
            "/api/expressions/compile",
            json={"expression": "@a + 1", "engine": "python"},
        )
        assert resp.status_code == 200
        assert resp.json()["valid"] is False


class TestDraftValidationWithExpressions:
    def _setup(self, client, tmp_path, catalog_dir):
        client.post("/api/sources", json={
            "name": "src",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "users": Table(name="users", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="first_name", data_type="text"),
                Column(name="last_name", data_type="text"),
            ], primary_key=["id"]),
        })
        mgr.create_snapshot("src", schema)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(
            ConfigManager.generate_default_config(schema), mapping_path
        )
        client.post("/api/projects", json={
            "name": "proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })

    def test_draft_with_bad_expression_is_reported(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post(
            "/api/projects/proj/validate-draft",
            json={
                "source_schema": "public",
                "collections": {
                    "users": {
                        "source_table": "users",
                        "target_collection": "users",
                        "field_expressions": [
                            {
                                "target": "full_name",
                                "sources": ["first_name", "last_name"],
                                "expression": "UNKNOWN_FN(@first_name)",
                            }
                        ],
                    }
                },
                "edges": [],
                "type_overrides": {},
                "key_separator": "_",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is False
        assert any("full_name" in m and "UNKNOWN_FN" in m for m in body["issues"])

    def test_draft_with_valid_expression_passes(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post(
            "/api/projects/proj/validate-draft",
            json={
                "source_schema": "public",
                "collections": {
                    "users": {
                        "source_table": "users",
                        "target_collection": "users",
                        "field_expressions": [
                            {
                                "target": "full_name",
                                "sources": ["first_name", "last_name"],
                                "expression": 'CONCAT(@first_name, " ", @last_name)',
                            }
                        ],
                    }
                },
                "edges": [],
                "type_overrides": {},
                "key_separator": "_",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is True, body

    def test_draft_expression_referencing_unknown_column_is_reported(
        self, client, tmp_path, catalog_dir
    ):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post(
            "/api/projects/proj/validate-draft",
            json={
                "source_schema": "public",
                "collections": {
                    "users": {
                        "source_table": "users",
                        "target_collection": "users",
                        "field_expressions": [
                            {
                                "target": "oops",
                                "sources": [],
                                "expression": "UPPER(@nope)",
                            }
                        ],
                    }
                },
                "edges": [],
                "type_overrides": {},
                "key_separator": "_",
            },
        )
        body = resp.json()
        assert body["valid"] is False
        assert any("@nope" in m for m in body["issues"])
