"""CLI tests for ``r2g catalog resync-classifications`` (PRD Phase 9c)."""
from __future__ import annotations

import sys

import pytest
from typer.testing import CliRunner

from r2g.catalog import CatalogManager
from r2g.catalogs.base import CatalogAsset, ResolvedSource
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


class _FakeProvider:
    """Returns a resolved source whose customer.email is newly classified PII."""

    def get_asset(self, fqn):
        return CatalogAsset(
            provider="om",
            provider_type="openmetadata",
            fqn=fqn,
            kind="database",
            name="shop",
            source_type="postgresql",
        )

    def resolve_source(self, asset):
        return ResolvedSource(
            source_type="postgresql",
            connection_string="postgresql://localhost/shop",
            column_classifications={
                "customer": {"email": Classification(tags=["PII.Sensitive"])}
            },
            owners=["data-team@x.io"],
            tier="Tier.Tier1",
        )


@pytest.fixture
def bound_source(tmp_path, monkeypatch):
    catalog_dir = tmp_path / "catalog"
    mgr = CatalogManager(str(catalog_dir))
    # A source imported from a catalog (provenance set), initially unclassified.
    mgr.add_source(
        "shop",
        "postgresql",
        "postgresql://localhost/shop",
        catalog_name="om",
        catalog_asset_fqn="svc.shop.public",
    )
    schema = Schema(tables={
        "customer": Table(
            name="customer",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="email", data_type="text"),
            ],
            primary_key=["id"],
        ),
    })
    mgr.create_snapshot("shop", schema, pg_schema="public")
    monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(str(catalog_dir)))
    monkeypatch.setattr("r2g.main._get_catalog_provider", lambda mgr, name: _FakeProvider())
    return catalog_dir


class TestResyncClassifications:
    def test_resync_updates_source_and_snapshot(self, bound_source):
        result = runner.invoke(app, ["catalog", "resync-classifications", "shop"])
        assert result.exit_code == 0, result.output
        assert "Re-synced" in result.output

        mgr = CatalogManager(str(bound_source))
        source = mgr.get_source("shop")
        assert source.classifications["customer"]["email"].tags == ["PII.Sensitive"]
        assert source.data_owners == ["data-team@x.io"]
        assert source.data_tier == "Tier.Tier1"
        assert source.classifications_synced_at is not None

        snap = mgr.get_latest_snapshot("shop")
        email = next(c for c in snap.schema_data.tables["customer"].columns if c.name == "email")
        assert email.classification is not None
        assert email.classification.tags == ["PII.Sensitive"]

    def test_unbound_source_exits_1(self, tmp_path, monkeypatch):
        catalog_dir = tmp_path / "catalog"
        mgr = CatalogManager(str(catalog_dir))
        mgr.add_source("plain", "postgresql", "postgresql://localhost/x")
        monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(str(catalog_dir)))
        result = runner.invoke(app, ["catalog", "resync-classifications", "plain"])
        assert result.exit_code == 1
        assert "not imported from a catalog" in result.output

    def test_unknown_source_exits_1(self, tmp_path, monkeypatch):
        catalog_dir = tmp_path / "catalog"
        CatalogManager(str(catalog_dir))
        monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(str(catalog_dir)))
        result = runner.invoke(app, ["catalog", "resync-classifications", "nope"])
        assert result.exit_code == 1
        assert "not found" in result.output.lower()
