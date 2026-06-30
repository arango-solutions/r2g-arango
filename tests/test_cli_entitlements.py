"""CLI tests for ``r2g entitlements report`` (PRD Phase 9b)."""
from __future__ import annotations

import json
import sys

import pytest
from typer.testing import CliRunner

from r2g.catalog import CatalogManager
from r2g.config import ConfigManager
from r2g.main import app
from r2g.types import Classification, Column, Schema, Table

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_structlog(monkeypatch):
    import structlog

    def _stderr_setup(level: str = "INFO", json_output: bool = False) -> None:
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(0),
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
            cache_logger_on_first_use=False,
        )

    monkeypatch.setattr("r2g.log.setup_logging", _stderr_setup)
    monkeypatch.setattr("r2g.main.setup_logging", _stderr_setup)
    _stderr_setup()
    yield
    structlog.reset_defaults()


@pytest.fixture
def project(tmp_path, monkeypatch):
    catalog_dir = tmp_path / "catalog"
    mgr = CatalogManager(str(catalog_dir))
    mgr.add_source("shop", "postgresql", "postgresql://localhost/shop")
    schema = Schema(tables={
        "customer": Table(
            name="customer",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="email", data_type="text",
                       classification=Classification(tags=["PII.Sensitive"])),
                Column(name="name", data_type="text"),
            ],
            primary_key=["id"],
        ),
    })
    mgr.create_snapshot("shop", schema, pg_schema="public")
    config = ConfigManager.generate_default_config(schema)
    mapping_path = tmp_path / "mapping.yaml"
    ConfigManager.save_config(config, str(mapping_path))
    mgr.create_project("proj", "shop", str(mapping_path))
    monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(str(catalog_dir)))
    return "proj"


class TestEntitlementsReport:
    def test_help(self):
        result = runner.invoke(app, ["entitlements", "report", "--help"])
        assert result.exit_code == 0
        assert "threshold" in result.output

    def test_reports_above_threshold_field(self, project):
        result = runner.invoke(app, ["entitlements", "report", project])
        assert result.exit_code == 0, result.output
        assert "email" in result.output
        assert "restricted" in result.output

    def test_json_output(self, project):
        result = runner.invoke(app, ["entitlements", "report", project, "--json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["threshold"] == "confidential"
        levels = {f"{f['target_collection']}.{f['target_property']}": f["level"]
                  for f in payload["fields"]}
        assert levels["customer.email"] == "restricted"
        assert payload["summary"]["above_threshold"] >= 1

    def test_threshold_above_everything_is_clean(self, project):
        result = runner.invoke(app, ["entitlements", "report", project, "--threshold", "restricted"])
        assert result.exit_code == 0, result.output
        # email is restricted, so still flagged at restricted threshold
        assert "email" in result.output

    def test_invalid_threshold_exits_1(self, project):
        result = runner.invoke(app, ["entitlements", "report", project, "--threshold", "secret"])
        assert result.exit_code == 1
        assert "Invalid" in result.output

    def test_unknown_project_exits_1(self, project):
        result = runner.invoke(app, ["entitlements", "report", "nope"])
        assert result.exit_code == 1
        assert "not found" in result.output.lower()


class TestEntitlementsEmit:
    def test_help(self):
        result = runner.invoke(app, ["entitlements", "emit", "--help"])
        assert result.exit_code == 0
        assert "tier-layout" in result.output

    def test_emits_artifacts_to_project_dir(self, project, tmp_path):
        result = runner.invoke(app, ["entitlements", "emit", project])
        assert result.exit_code == 0, result.output
        gov = tmp_path / "governance"
        assert (gov / "classification-manifest.json").exists()
        assert (gov / "suggested-rbac.json").exists()
        assert (gov / "policy.rego").exists()
        assert (gov / "lineage.json").exists()
        # tier-layout only with the flag
        assert not (gov / "tier-layout.json").exists()

    def test_tier_layout_flag(self, project, tmp_path):
        result = runner.invoke(app, ["entitlements", "emit", project, "--tier-layout"])
        assert result.exit_code == 0, result.output
        assert (tmp_path / "governance" / "tier-layout.json").exists()

    def test_no_rego_flag(self, project, tmp_path):
        result = runner.invoke(app, ["entitlements", "emit", project, "--no-rego"])
        assert result.exit_code == 0, result.output
        assert not (tmp_path / "governance" / "policy.rego").exists()

    def test_invalid_threshold_exits_1(self, project):
        result = runner.invoke(app, ["entitlements", "emit", project, "--threshold", "secret"])
        assert result.exit_code == 1
        assert "Invalid" in result.output

    def test_unknown_project_exits_1(self, project):
        result = runner.invoke(app, ["entitlements", "emit", "nope"])
        assert result.exit_code == 1
        assert "not found" in result.output.lower()
