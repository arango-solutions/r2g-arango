"""CLI tests for the `r2g catalog` group (Phase 8).

Each test performs exactly ONE CliRunner invocation (prerequisites are arranged
directly via CatalogManager). This avoids the structlog/stdout state that leaks
across multiple invocations in a single test. `_get_catalog` is patched to a
temp-dir manager so the real ~/.r2g is never touched; the provider is patched
to a fake so no network is hit.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import click
import pytest
from typer.testing import CliRunner

from r2g.catalog import CatalogManager
from r2g.catalogs.base import CatalogAsset, ResolvedSource
from r2g.main import app

runner = CliRunner()


def plain_output(result) -> str:
    return click.unstyle(result.output)


@pytest.fixture(autouse=True)
def _silence_loggers():
    with patch("r2g.main.log", MagicMock()), patch("r2g.catalog.logger", MagicMock()):
        yield
    import structlog

    structlog.reset_defaults()


@pytest.fixture
def mgr(tmp_path):
    """A temp-dir CatalogManager, also patched in as `_get_catalog`."""
    manager = CatalogManager(str(tmp_path))
    with patch("r2g.main._get_catalog", lambda: CatalogManager(str(tmp_path))):
        yield manager


class _FakeProvider:
    provider_type = "openmetadata"
    name = "corp"

    def list_data_sources(self):
        return [
            CatalogAsset(
                provider="corp", provider_type="openmetadata", fqn="pg",
                kind="service", name="pg", source_type="postgresql",
            )
        ]

    def list_children(self, asset):
        return [
            CatalogAsset(
                provider="corp", provider_type="openmetadata", fqn="pg.shop",
                kind="database", name="shop", source_type="postgresql",
                connection_hint={"database": "shop"},
            )
        ]

    def search(self, query, *, limit=50):
        return self.list_children(None)

    def get_asset(self, fqn):
        return CatalogAsset(
            provider="corp", provider_type="openmetadata", fqn=fqn,
            kind="database", name="shop", source_type="postgresql",
            connection_hint={"database": "shop"},
        )

    def resolve_source(self, asset):
        return ResolvedSource(
            source_type="postgresql",
            connection_string="postgresql://$R2G_DB_USER:$R2G_DB_PASSWORD@warehouse.db:5432/shop",
            notes="Credentials are not read from the catalog; set $R2G_DB_USER / $R2G_DB_PASSWORD.",
        )


def _patch_provider():
    return patch("r2g.catalogs.base.create_catalog_provider", return_value=_FakeProvider())


class TestCatalogAdd:
    def test_add_persists_catalog(self, mgr):
        r = runner.invoke(app, [
            "catalog", "add", "--name", "corp", "--type", "openmetadata",
            "--endpoint", "http://localhost:8585",
        ])
        assert r.exit_code == 0, plain_output(r)
        assert "added" in plain_output(r)
        assert mgr.get_catalog("corp").endpoint == "http://localhost:8585"

    def test_add_unsupported_type_exits_1(self, mgr):
        r = runner.invoke(app, [
            "catalog", "add", "--name", "x", "--type", "collibra", "--endpoint", "http://h",
        ])
        assert r.exit_code == 1
        assert "Unsupported catalog provider type" in plain_output(r)


class TestCatalogList:
    def test_list_shows_registered(self, mgr):
        mgr.add_catalog("corp", "openmetadata", "http://localhost:8585")
        r = runner.invoke(app, ["catalog", "list"])
        assert r.exit_code == 0, plain_output(r)
        assert "corp" in plain_output(r)
        assert "http://localhost:8585" in plain_output(r)

    def test_list_empty(self, mgr):
        r = runner.invoke(app, ["catalog", "list"])
        assert r.exit_code == 0
        assert "No catalogs registered" in plain_output(r)


class TestCatalogRemove:
    def test_remove_existing(self, mgr):
        mgr.add_catalog("corp", "openmetadata", "http://h:8585")
        r = runner.invoke(app, ["catalog", "remove", "corp"])
        assert r.exit_code == 0
        assert "removed" in plain_output(r)
        assert mgr.get_catalog("corp") is None

    def test_remove_missing_exits_1(self, mgr):
        r = runner.invoke(app, ["catalog", "remove", "ghost"])
        assert r.exit_code == 1


class TestCatalogBrowse:
    def test_browse_top_level(self, mgr):
        mgr.add_catalog("corp", "openmetadata", "http://localhost:8585")
        with _patch_provider():
            r = runner.invoke(app, ["catalog", "browse", "corp"])
        out = plain_output(r)
        assert r.exit_code == 0, out
        assert "pg" in out and "postgresql" in out

    def test_browse_unknown_catalog_exits_1(self, mgr):
        with _patch_provider():
            r = runner.invoke(app, ["catalog", "browse", "nope"])
        assert r.exit_code == 1
        assert "not found" in plain_output(r)


class TestCatalogImportSource:
    def test_import_creates_source(self, mgr):
        mgr.add_catalog("corp", "openmetadata", "http://localhost:8585")
        with _patch_provider():
            r = runner.invoke(app, [
                "catalog", "import-source", "corp", "pg.shop", "--as", "shop_src",
            ])
        out = plain_output(r)
        assert r.exit_code == 0, out
        assert "imported" in out
        assert "$R2G_DB_PASSWORD" in out  # credential placeholder surfaced to the user

        src = mgr.get_source("shop_src")
        assert src is not None
        assert src.source_type == "postgresql"
        assert "$R2G_DB_USER" in src.connection_string
