"""CLI test for classification merge at ``r2g source snapshot`` (PRD Phase 9a).

A source imported from a catalog persists ``SourceConfig.classifications``; the
snapshot command must stamp those onto the introspected ``Column.classification``
without affecting plain (non-catalog) sources. The connector is faked so no live
database is needed.
"""
from __future__ import annotations

import sys
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from r2g.catalog import CatalogManager
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


def _schema() -> Schema:
    return Schema(tables={
        "customer": Table(
            name="customer",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="email", data_type="text"),
            ],
            primary_key=["id"],
        ),
    })


class _FakeConnector:
    def __init__(self, *a, **kw) -> None:
        pass

    def get_schema(self) -> Schema:
        return _schema()


def test_snapshot_merges_classifications(tmp_path, monkeypatch):
    catalog_dir = tmp_path / "catalog"
    mgr = CatalogManager(str(catalog_dir))
    mgr.add_source(
        "shop",
        "postgresql",
        "postgresql://localhost/shop",
        classifications={"customer": {"email": Classification(tags=["PII.Sensitive"])}},
    )
    monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(str(catalog_dir)))

    with patch("r2g.connectors.base.create_source_connector", lambda *a, **kw: _FakeConnector()):
        result = runner.invoke(app, ["source", "snapshot", "shop"])

    assert result.exit_code == 0, result.output
    assert "Annotated 1 column" in result.output

    snap = CatalogManager(str(catalog_dir)).get_latest_snapshot("shop")
    assert snap is not None
    cols = {c.name: c for c in snap.schema_data.tables["customer"].columns}
    assert cols["email"].classification is not None
    assert cols["email"].classification.tags == ["PII.Sensitive"]
    assert cols["id"].classification is None


def test_snapshot_plain_source_unannotated(tmp_path, monkeypatch):
    catalog_dir = tmp_path / "catalog"
    mgr = CatalogManager(str(catalog_dir))
    mgr.add_source("plain", "postgresql", "postgresql://localhost/db")
    monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(str(catalog_dir)))

    with patch("r2g.connectors.base.create_source_connector", lambda *a, **kw: _FakeConnector()):
        result = runner.invoke(app, ["source", "snapshot", "plain"])

    assert result.exit_code == 0, result.output
    assert "Annotated" not in result.output
    snap = CatalogManager(str(catalog_dir)).get_latest_snapshot("plain")
    assert all(
        c.classification is None
        for t in snap.schema_data.tables.values()
        for c in t.columns
    )
