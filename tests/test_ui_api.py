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

    def test_add_snowflake_source_is_accepted(self, client):
        resp = client.post("/api/sources", json={
            "name": "analytics_sf",
            "source_type": "snowflake",
            "connection_string": (
                "snowflake://svc:x@xy12345.us-east-1/ANALYTICS/CORE"
                "?warehouse=WH&role=R"
            ),
        })
        assert resp.status_code == 201
        body = resp.json()
        assert body["source_type"] == "snowflake"
        # connection string should be redacted
        assert "svc:x@" not in body["connection_string"]

    def test_add_csv_source_and_snapshot(self, client, tmp_path):
        csv_dir = tmp_path / "dumps"
        csv_dir.mkdir()
        (csv_dir / "customers.csv").write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
        resp = client.post("/api/sources", json={
            "name": "csv_src",
            "source_type": "csv",
            "connection_string": str(csv_dir),
            "source_params": {"delimiter": ",", "has_header": True},
        })
        assert resp.status_code == 201
        assert resp.json()["source_type"] == "csv"

        snap = client.post("/api/sources/csv_src/snapshot")
        assert snap.status_code == 200, snap.text
        assert snap.json()["tables"] == 1

        schema = client.get("/api/sources/csv_src/schema")
        assert schema.status_code == 200
        assert "customers" in schema.json()["tables"]

    def test_add_kafka_source_accepted(self, client):
        resp = client.post("/api/sources", json={
            "name": "kafka_src",
            "source_type": "kafka",
            "connection_string": "localhost:9092",
            "source_params": {"schema_registry_url": "http://localhost:8081", "topic": "orders"},
        })
        assert resp.status_code == 201
        assert resp.json()["source_type"] == "kafka"

    def test_add_source_rejects_unsupported_type(self, client):
        resp = client.post("/api/sources", json={
            "name": "my_oracle",
            "source_type": "oracle",
            "connection_string": "oracle://u:p@h/db",
        })
        assert resp.status_code in (400, 409)
        assert "oracle" in resp.text.lower() or "unsupported" in resp.text.lower()

    def test_snapshot_snowflake_without_driver_returns_501(self, client, monkeypatch):
        import sys

        client.post("/api/sources", json={
            "name": "analytics_sf",
            "source_type": "snowflake",
            "connection_string": (
                "snowflake://svc:x@xy12345/ANALYTICS/CORE?warehouse=WH"
            ),
        })
        monkeypatch.setitem(sys.modules, "snowflake", None)
        monkeypatch.setitem(sys.modules, "snowflake.connector", None)
        resp = client.post("/api/sources/analytics_sf/snapshot")
        assert resp.status_code == 501
        body = resp.json()
        detail = body.get("detail", "").lower()
        assert "r2g-arango[snowflake]" in detail or "snowflake-connector" in detail


class TestInferFksEndpoint:
    def _setup(self, client, catalog_dir):
        client.post("/api/sources", json={
            "name": "src",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "users": Table(name="users", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
            ], primary_key=["id"]),
            "orders": Table(name="orders", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="user_id", data_type="integer"),
            ], primary_key=["id"]),
        })
        mgr.create_snapshot("src", schema)

    def test_infer_fks_returns_user_id_candidate(self, client, catalog_dir):
        self._setup(client, catalog_dir)
        resp = client.post("/api/sources/src/infer-fks")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "src"
        assert body["sample_used"] is False
        cands = body["candidates"]
        assert len(cands) == 1
        c = cands[0]
        assert c["table"] == "orders"
        assert c["columns"] == ["user_id"]
        assert c["foreign_table"] == "users"
        assert c["foreign_columns"] == ["id"]
        assert 0.0 <= c["confidence"] <= 1.0

    def test_infer_fks_without_snapshot_returns_400(self, client):
        client.post("/api/sources", json={
            "name": "empty",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        resp = client.post("/api/sources/empty/infer-fks")
        assert resp.status_code == 400
        assert "snapshot" in resp.json()["detail"].lower()

    def test_infer_fks_unknown_source_returns_404(self, client):
        resp = client.post("/api/sources/nonexistent/infer-fks")
        assert resp.status_code == 404

    def test_infer_fks_min_confidence_filters_results(self, client, catalog_dir):
        self._setup(client, catalog_dir)
        resp = client.post(
            "/api/sources/src/infer-fks",
            json={"min_confidence": 0.99},
        )
        assert resp.status_code == 200
        assert resp.json()["candidates"] == []

    def test_analyze_denorm_reports_repeating_group(self, client, catalog_dir):
        client.post("/api/sources", json={
            "name": "rg",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "contact": Table(name="contact", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="phone1", data_type="text"),
                Column(name="phone2", data_type="text"),
            ], primary_key=["id"]),
        })
        mgr.create_snapshot("rg", schema)
        resp = client.post("/api/sources/rg/analyze-denorm")
        assert resp.status_code == 200
        body = resp.json()
        assert body["source"] == "rg"
        assert body["sample_used"] is False
        kinds = {f["kind"] for f in body["findings"]}
        assert "repeating_group" in kinds

    def test_analyze_denorm_without_snapshot_returns_400(self, client):
        client.post("/api/sources", json={
            "name": "empty2",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        resp = client.post("/api/sources/empty2/analyze-denorm")
        assert resp.status_code == 400

    def test_analyze_denorm_unknown_source_returns_404(self, client):
        resp = client.post("/api/sources/nope/analyze-denorm")
        assert resp.status_code == 404

    def test_analyze_denorm_skips_sampling_for_snowflake_source(self, client, catalog_dir):
        client.post("/api/sources", json={
            "name": "sf2",
            "source_type": "snowflake",
            "connection_string": "snowflake://u:p@acct/DB/PUBLIC",
        })
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "CONTACT": Table(name="CONTACT", columns=[
                Column(name="ID", data_type="number", is_primary_key=True),
                Column(name="PHONE1", data_type="text"),
                Column(name="PHONE2", data_type="text"),
            ], primary_key=["ID"]),
        })
        mgr.create_snapshot("sf2", schema, pg_schema="PUBLIC")
        resp = client.post("/api/sources/sf2/analyze-denorm", json={"sample": True})
        assert resp.status_code == 200
        # Snowflake has no value sampler today; structural detectors still run.
        assert resp.json()["sample_used"] is False

    def test_infer_fks_skips_sampling_for_snowflake_source(self, client, catalog_dir):
        client.post("/api/sources", json={
            "name": "sf",
            "source_type": "snowflake",
            "connection_string": "snowflake://u:p@acct/DB/PUBLIC",
        })
        mgr = CatalogManager(catalog_dir)
        schema = Schema(tables={
            "USERS": Table(
                name="USERS",
                columns=[Column(name="ID", data_type="number", is_primary_key=True)],
                primary_key=["ID"],
            ),
            "ORDERS": Table(
                name="ORDERS",
                columns=[
                    Column(name="ID", data_type="number", is_primary_key=True),
                    Column(name="USER_ID", data_type="number"),
                ],
                primary_key=["ID"],
            ),
        })
        mgr.create_snapshot("sf", schema, pg_schema="PUBLIC")
        resp = client.post("/api/sources/sf/infer-fks", json={"sample": True})
        assert resp.status_code == 200
        body = resp.json()
        # Sampling is PG-only today — requesting it on a Snowflake source
        # must not crash and must not claim sampling was used.
        assert body["sample_used"] is False
        assert len(body["candidates"]) >= 1


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

    def test_create_project_rejects_traversal_name(self, client):
        self._add_source(client)
        resp = client.post("/api/projects", json={
            "name": "../escape",
            "source_name": "src",
        })
        assert resp.status_code == 400

    def test_mapping_path_is_confined_to_catalog_dir(self, client, tmp_path, catalog_dir):
        """A client-supplied mapping path must never be used as a write target."""
        self._add_source(client)
        evil = tmp_path / "evil.yaml"  # outside the catalog dir
        resp = client.post("/api/projects", json={
            "name": "confined",
            "source_name": "src",
            "mapping_config_path": str(evil),
        })
        assert resp.status_code == 201
        stored = resp.json()["mapping_config_path"]
        # Persisted path is derived under <catalog>/projects/<name>/, not the
        # client path.
        assert str(catalog_dir) in stored
        assert stored.endswith("projects/confined/mapping.yaml")
        # Saving the mapping writes to the safe path, never to the evil path.
        client.put("/api/projects/confined/mapping", json=MappingConfig().model_dump())
        assert not evil.exists()

    def test_create_project_with_metadata(self, client, tmp_path):
        self._add_source(client)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(MappingConfig(), mapping_path)
        resp = client.post("/api/projects", json={
            "name": "meta_proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
            "mapping_description": "A described mapping",
        })
        assert resp.status_code == 201
        assert resp.json()["mapping_description"] == "A described mapping"

    def test_patch_project_metadata(self, client, tmp_path):
        self._add_source(client)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(MappingConfig(), mapping_path)
        client.post("/api/projects", json={
            "name": "patch_proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })
        resp = client.patch("/api/projects/patch_proj", json={
            "mapping_name": "Renamed",
            "mapping_description": "new desc",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["mapping_name"] == "Renamed"
        assert body["mapping_description"] == "new desc"

    def test_patch_project_not_found(self, client):
        resp = client.patch("/api/projects/nope", json={"mapping_name": "x"})
        assert resp.status_code == 404

    def test_patch_project_no_fields(self, client, tmp_path):
        self._add_source(client)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(MappingConfig(), mapping_path)
        client.post("/api/projects", json={
            "name": "empty_patch",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })
        resp = client.patch("/api/projects/empty_patch", json={})
        assert resp.status_code == 400

    def test_delete_project(self, client, tmp_path):
        self._add_source(client)
        mapping_path = str(tmp_path / "mapping.yaml")
        ConfigManager.save_config(MappingConfig(), mapping_path)
        client.post("/api/projects", json={
            "name": "del_proj",
            "source_name": "src",
            "mapping_config_path": mapping_path,
        })
        resp = client.delete("/api/projects/del_proj")
        assert resp.status_code == 200
        assert resp.json() == {"deleted": "del_proj"}
        # Gone from the listing.
        names = [p["name"] for p in client.get("/api/projects").json()]
        assert "del_proj" not in names

    def test_delete_project_not_found(self, client):
        resp = client.delete("/api/projects/nope")
        assert resp.status_code == 404


class TestApiAuth:
    def _app_client(self, catalog_dir, **kwargs):
        from starlette.testclient import TestClient

        return TestClient(create_app(catalog_dir=catalog_dir, **kwargs))

    def test_loopback_no_token_is_open(self, catalog_dir):
        client = self._app_client(catalog_dir)
        assert client.get("/api/sources").status_code == 200

    def test_token_required_when_configured(self, catalog_dir):
        client = self._app_client(catalog_dir, api_token="s3cr3t")
        assert client.get("/api/sources").status_code == 401
        assert client.get("/api/health").status_code == 200  # health stays open
        ok = client.get("/api/sources", headers={"Authorization": "Bearer s3cr3t"})
        assert ok.status_code == 200

    def test_non_loopback_bind_generates_token(self, catalog_dir):
        app = create_app(catalog_dir=catalog_dir, host="0.0.0.0")
        assert app.state.api_auth_required is True
        assert app.state.api_token


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

    def test_apply_naming_collections_pascal(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/proj/apply-naming", json={"collections": "pascal"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["collections"]["users"]["target_collection"] == "Users"
        assert data["naming_convention"]["collections"] == "pascal"

    def test_apply_naming_properties_camel(self, client, tmp_path, catalog_dir):
        client.post("/api/sources", json={
            "name": "src2",
            "source_type": "postgresql",
            "connection_string": "postgresql://localhost/test",
        })
        schema = Schema(tables={
            "order_line": Table(name="order_line", columns=[
                Column(name="line_id", data_type="integer", is_primary_key=True),
                Column(name="unit_price", data_type="numeric"),
            ], primary_key=["line_id"]),
        })
        config = ConfigManager.generate_default_config(schema)
        mapping_path = str(tmp_path / "mapping2.yaml")
        ConfigManager.save_config(config, mapping_path)
        CatalogManager(catalog_dir).create_snapshot("src2", schema)
        client.post("/api/projects", json={
            "name": "proj2", "source_name": "src2", "mapping_config_path": mapping_path,
        })
        resp = client.post(
            "/api/projects/proj2/apply-naming",
            json={"collections": "pascal", "properties": "camel"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["collections"]["order_line"]["target_collection"] == "OrderLine"
        fm = data["collections"]["order_line"]["field_mappings"]
        assert fm["unit_price"] == "unitPrice"

    def test_apply_naming_not_found(self, client):
        resp = client.post("/api/projects/nope/apply-naming", json={"collections": "pascal"})
        assert resp.status_code == 404

    def test_migration_plan_when_never_loaded(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.get("/api/projects/proj/migration-plan")
        assert resp.status_code == 200
        assert resp.json()["loaded"] is False

    def test_migrate_when_never_loaded_rejected(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/proj/migrate", json={"dry_run": True})
        assert resp.status_code == 400

    def test_migration_plan_detects_property_rename(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        # Pretend the current mapping was loaded into the DB.
        loaded = client.get("/api/projects/proj/mapping").json()
        CatalogManager(catalog_dir).set_loaded_mapping("proj", loaded)
        # Save an edited mapping that renames a property.
        edited = client.get("/api/projects/proj/mapping").json()
        edited["collections"]["users"]["field_mappings"] = {"name": "fullName"}
        assert client.put("/api/projects/proj/mapping", json=edited).status_code == 200

        resp = client.get("/api/projects/proj/migration-plan")
        assert resp.status_code == 200
        data = resp.json()
        assert data["loaded"] is True
        kinds = {c["change_type"] for c in data["plan"]["changes"]}
        assert "field_mapping_added" in kinds

    def test_migrate_dry_run_reports_plan(self, client, tmp_path, catalog_dir):
        self._setup_project(client, tmp_path, catalog_dir)
        # Point at a real (non-system) database so the migration guard passes.
        CatalogManager(catalog_dir).update_project("proj", arango_database="proj_graph")
        loaded = client.get("/api/projects/proj/mapping").json()
        CatalogManager(catalog_dir).set_loaded_mapping("proj", loaded)
        edited = client.get("/api/projects/proj/mapping").json()
        edited["collections"]["users"]["field_mappings"] = {"name": "fullName"}
        client.put("/api/projects/proj/mapping", json=edited)

        resp = client.post("/api/projects/proj/migrate", json={"dry_run": True})
        assert resp.status_code == 200
        body = resp.json()
        assert body["migrated"] is False  # dry run never reports success
        assert len(body["report"]["actions_skipped"]) >= 1

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


class TestSecretRedaction:
    def test_source_connection_string_is_redacted(self, client):
        resp = client.post("/api/sources", json={
            "name": "pg",
            "source_type": "postgresql",
            "connection_string": "postgresql://u:hunter2@db.example.com:5432/app",
        })
        assert resp.status_code == 201
        body = resp.json()
        assert "hunter2" not in body["connection_string"]
        assert "u:***@db.example.com:5432/app" in body["connection_string"]

        lst = client.get("/api/sources").json()
        assert "hunter2" not in lst[0]["connection_string"]

    def test_target_password_is_redacted(self, client):
        resp = client.post("/api/targets", json={
            "name": "arango",
            "endpoint": "http://localhost:8529",
            "database": "_system",
            "username": "root",
            "password": "VERY-SECRET-VALUE",
        })
        assert resp.status_code == 201
        body = resp.json()
        assert "VERY-SECRET-VALUE" not in body["password"]
        assert body["password"].startswith("***")

        lst = client.get("/api/targets").json()
        assert "VERY-SECRET-VALUE" not in lst[0]["password"]


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

    def test_preview_evaluates_against_sample_row(self, client):
        resp = client.post(
            "/api/expressions/preview",
            json={
                "expression": 'CONCAT(UPPER(@first), " ", @last)',
                "row": {"first": "ada", "last": "Lovelace"},
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is True
        assert body["result"] == "ADA Lovelace"
        assert body["references"] == ["first", "last"]

    def test_preview_invalid_expression_returns_error(self, client):
        resp = client.post(
            "/api/expressions/preview",
            json={"expression": "UNKNOWN_FN(@a)", "row": {}},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is False
        assert "UNKNOWN_FN" in body["error"]

    def test_preview_missing_row_yields_null_passthrough(self, client):
        resp = client.post(
            "/api/expressions/preview",
            json={"expression": "@missing"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["valid"] is True
        assert body["result"] is None

    def test_preview_rejects_non_aql_engine(self, client):
        resp = client.post(
            "/api/expressions/preview",
            json={"expression": "@a", "engine": "python", "row": {}},
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


class TestStaticAssetSafety:
    def test_no_inline_handler_js_string_interpolation(self):
        """Guard against DOM XSS: inline event handlers must not interpolate a
        value into a JS string literal (e.g. onclick="fn('${x}')"), since that
        vector survives HTML-escaping (the parser decodes entities before the JS
        runs). Untrusted values must flow through data-* + this.dataset instead.
        """
        import re

        from r2g.ui.server import _STATIC_DIR

        html = (_STATIC_DIR / "index.html").read_text(encoding="utf-8")
        pat = re.compile(r"on\w+=\"[^\"]*'\$\{")
        offenders = [
            ln.strip()
            for ln in html.splitlines()
            if pat.search(ln) and not ln.lstrip().startswith("//")
        ]
        assert offenders == [], offenders


class TestCatalogEndpoints:
    """External data catalog discovery endpoints (Phase 8b)."""

    def _fake_provider(self):
        from r2g.catalogs.base import CatalogAsset, ResolvedSource

        class _Fake:
            provider_type = "openmetadata"
            name = "corp"

            def list_data_sources(self):
                return [CatalogAsset(
                    provider="corp", provider_type="openmetadata", fqn="pg",
                    kind="service", name="pg", source_type="postgresql",
                )]

            def list_children(self, asset):
                return [CatalogAsset(
                    provider="corp", provider_type="openmetadata", fqn="pg.shop",
                    kind="database", name="shop", source_type="postgresql",
                )]

            def search(self, q, *, limit=50):
                return self.list_children(None)

            def get_asset(self, fqn):
                return CatalogAsset(
                    provider="corp", provider_type="openmetadata", fqn=fqn,
                    kind="database", name="shop", source_type="postgresql",
                )

            def resolve_source(self, asset):
                return ResolvedSource(
                    source_type="postgresql",
                    connection_string="postgresql://$R2G_DB_USER:$R2G_DB_PASSWORD@h:5432/shop",
                    notes="set creds via env",
                )

        return _Fake()

    def test_add_and_list_catalog_redacts_token(self, client):
        resp = client.post("/api/catalogs", json={
            "name": "corp", "provider_type": "openmetadata",
            "endpoint": "http://localhost:8585", "token": "supersecret",
        })
        assert resp.status_code == 201
        listing = client.get("/api/catalogs").json()
        assert listing[0]["name"] == "corp"
        assert "supersecret" not in listing[0]["token"]  # redacted

    def test_add_unsupported_type_409(self, client):
        resp = client.post("/api/catalogs", json={
            "name": "x", "provider_type": "collibra", "endpoint": "http://h",
        })
        assert resp.status_code == 409

    def test_remove_catalog(self, client):
        client.post("/api/catalogs", json={
            "name": "corp", "provider_type": "openmetadata", "endpoint": "http://h:8585",
        })
        assert client.delete("/api/catalogs/corp").status_code == 200
        assert client.delete("/api/catalogs/corp").status_code == 404

    def test_browse(self, client, monkeypatch):
        client.post("/api/catalogs", json={
            "name": "corp", "provider_type": "openmetadata", "endpoint": "http://h:8585",
        })
        monkeypatch.setattr(
            "r2g.catalogs.base.create_catalog_provider",
            lambda *a, **k: self._fake_provider(),
        )
        resp = client.get("/api/catalogs/corp/browse")
        assert resp.status_code == 200
        assert resp.json()[0]["name"] == "pg"

    def test_browse_unknown_catalog_404(self, client):
        assert client.get("/api/catalogs/nope/browse").status_code == 404

    def test_import_source_creates_source(self, client, monkeypatch):
        client.post("/api/catalogs", json={
            "name": "corp", "provider_type": "openmetadata", "endpoint": "http://h:8585",
        })
        monkeypatch.setattr(
            "r2g.catalogs.base.create_catalog_provider",
            lambda *a, **k: self._fake_provider(),
        )
        resp = client.post("/api/catalogs/corp/import-source", json={
            "asset_fqn": "pg.shop", "source_name": "shop_src",
        })
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["source"]["name"] == "shop_src"
        # the imported source is now a normal source
        names = [s["name"] for s in client.get("/api/sources").json()]
        assert "shop_src" in names


class TestEntitlementsApi:
    """Phase 9b: GET /api/projects/{name}/entitlements."""

    def _setup(self, client, tmp_path, catalog_dir):
        from r2g.types import Classification

        schema = Schema(tables={
            "customer": Table(name="customer", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="email", data_type="text",
                       classification=Classification(tags=["PII.Sensitive"])),
                Column(name="name", data_type="text"),
            ], primary_key=["id"]),
        })
        config = ConfigManager.generate_default_config(schema)
        mapping_path = str(tmp_path / "ent_mapping.yaml")
        ConfigManager.save_config(config, mapping_path)
        mgr = CatalogManager(catalog_dir)
        mgr.add_source("esrc", "postgresql", "postgresql://localhost/db")
        mgr.create_snapshot("esrc", schema)
        client.post("/api/projects", json={
            "name": "eproj", "source_name": "esrc", "mapping_config_path": mapping_path,
        })

    def test_report_flags_pii(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.get("/api/projects/eproj/entitlements")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["threshold"] == "confidential"
        above = {f["target_property"] for f in data["above_threshold"]}
        assert "email" in above
        levels = {f["target_property"]: f["level"] for f in data["fields"]}
        assert levels["email"] == "restricted"
        assert levels["name"] == "public"

    def test_unknown_project_404(self, client):
        assert client.get("/api/projects/nope/entitlements").status_code == 404

    def test_invalid_threshold_400(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.get("/api/projects/eproj/entitlements?threshold=secret")
        assert resp.status_code == 400


class TestGovernanceEmitApi:
    """Phase 9c: POST /api/projects/{name}/governance/emit."""

    def _setup(self, client, tmp_path, catalog_dir):
        from r2g.types import Classification

        schema = Schema(tables={
            "customer": Table(name="customer", columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="email", data_type="text",
                       classification=Classification(tags=["PII.Sensitive"])),
                Column(name="name", data_type="text"),
            ], primary_key=["id"]),
        })
        config = ConfigManager.generate_default_config(schema)
        mapping_path = str(tmp_path / "gov_mapping.yaml")
        ConfigManager.save_config(config, mapping_path)
        mgr = CatalogManager(catalog_dir)
        mgr.add_source("gsrc", "postgresql", "postgresql://localhost/db")
        mgr.create_snapshot("gsrc", schema)
        client.post("/api/projects", json={
            "name": "gproj", "source_name": "gsrc", "mapping_config_path": mapping_path,
        })
        return tmp_path

    def test_emit_writes_artifacts(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/gproj/governance/emit?tier_layout=true")
        assert resp.status_code == 200, resp.text
        data = resp.json()
        arts = data["artifacts"]
        assert "classification-manifest.json" in arts
        assert "suggested-rbac.json" in arts
        assert "policy.rego" in arts
        assert "tier-layout.json" in arts
        # the returned paths are authoritative (server may manage the project dir)
        from pathlib import Path as _P
        assert _P(arts["classification-manifest.json"]).exists()

    def test_unknown_project_404(self, client):
        assert client.post("/api/projects/nope/governance/emit").status_code == 404

    def test_invalid_threshold_400(self, client, tmp_path, catalog_dir):
        self._setup(client, tmp_path, catalog_dir)
        resp = client.post("/api/projects/gproj/governance/emit?threshold=secret")
        assert resp.status_code == 400
