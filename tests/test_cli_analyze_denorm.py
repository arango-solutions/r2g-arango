"""CLI tests for ``r2g source analyze-denorm`` (PRD Phase 11a).

These exercise the read-only structural path (repeating groups, no sampler) and
``--json`` output against an in-memory catalog, so they need no live database.
"""

from __future__ import annotations

import json
import sys

import pytest
from typer.testing import CliRunner

from r2g.catalog import CatalogManager
from r2g.main import app
from r2g.types import Column, Schema, Table

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
def catalog_with_repeating_group(tmp_path, monkeypatch):
    catalog_dir = tmp_path / "catalog"
    mgr = CatalogManager(str(catalog_dir))
    mgr.add_source("pg_src", "postgresql", "postgresql://localhost/test")
    schema = Schema(
        tables={
            "contact": Table(
                name="contact",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="phone1", data_type="text"),
                    Column(name="phone2", data_type="text"),
                    Column(name="phone3", data_type="text"),
                ],
                primary_key=["id"],
            ),
        }
    )
    mgr.create_snapshot("pg_src", schema, pg_schema="public")
    monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(str(catalog_dir)))
    return catalog_dir


class TestAnalyzeDenormCli:
    def test_help(self):
        result = runner.invoke(app, ["source", "analyze-denorm", "--help"])
        assert result.exit_code == 0
        assert "denormalization" in result.output.lower()

    def test_reports_repeating_group(self, catalog_with_repeating_group):
        result = runner.invoke(app, ["source", "analyze-denorm", "pg_src"])
        assert result.exit_code == 0
        # Rich wraps cell text at the test terminal width, so assert on the
        # (stable) table title; content is asserted via --json below.
        assert "Denormalization findings for 'pg_src'" in result.output

    def test_json_output(self, catalog_with_repeating_group):
        result = runner.invoke(app, ["source", "analyze-denorm", "pg_src", "--json"])
        assert result.exit_code == 0
        # console.print_json may pretty-print; parse the emitted JSON payload.
        payload = json.loads(result.output)
        assert any(f["kind"] == "repeating_group" for f in payload)

    def test_unknown_source(self, catalog_with_repeating_group):
        result = runner.invoke(app, ["source", "analyze-denorm", "ghost"])
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_high_threshold_filters_everything(self, catalog_with_repeating_group):
        result = runner.invoke(
            app, ["source", "analyze-denorm", "pg_src", "--min-confidence", "0.99"]
        )
        assert result.exit_code == 0
        assert "No denormalization findings" in result.output
