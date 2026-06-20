"""Integration test for the OpenMetadata catalog provider (Phase 8a).

Runs against a live OpenMetadata server (set OPENMETADATA_ENDPOINT, and
OPENMETADATA_TOKEN for secured installs); skipped automatically otherwise.

OpenMetadata's full stack (server + DB + search engine) is heavy, so it is run
as a *separately started* instance rather than from r2g's docker-compose — see
CONTRIBUTING.md. This test validates that the provider's assumptions about the
real OpenMetadata REST API hold (response shapes, auth, FQN navigation) and
that resolution never leaks credentials. It is tolerant of an empty catalog.
"""
from __future__ import annotations

from r2g.catalogs.base import ASSET_DATABASE, ASSET_SERVICE
from r2g.catalogs.openmetadata import OpenMetadataProvider

from .conftest import (
    OPENMETADATA_ENDPOINT,
    OPENMETADATA_TOKEN,
    requires_openmetadata,
)


def _provider() -> OpenMetadataProvider:
    return OpenMetadataProvider(
        OPENMETADATA_ENDPOINT, name="it", token=OPENMETADATA_TOKEN or None
    )


@requires_openmetadata
class TestOpenMetadataLive:
    def test_list_data_sources_returns_assets(self):
        with _provider() as p:
            assets = p.list_data_sources()
        assert isinstance(assets, list)
        for a in assets:
            assert a.kind == ASSET_SERVICE
            assert a.fqn  # every service has an FQN

    def test_descend_and_resolve_first_relational_source(self):
        """If the OM instance has any relational service, descend to a database
        and resolve it — asserting a usable source_type and NO leaked secret."""
        with _provider() as p:
            services = [
                a
                for a in p.list_data_sources()
                if a.source_type in ("postgresql", "mysql", "sqlserver", "snowflake")
            ]
            if not services:
                import pytest

                pytest.skip("no relational services registered in this OpenMetadata instance")

            databases: list = []
            for svc in services:
                databases = p.list_children(svc)
                if databases:
                    break
            if not databases:
                import pytest

                pytest.skip("no databases under any relational service")

            db = databases[0]
            assert db.kind == ASSET_DATABASE
            resolved = p.resolve_source(db)

        assert resolved.source_type in ("postgresql", "mysql", "sqlserver", "snowflake")
        # discover-then-connect: credentials are placeholders, never real secrets
        assert "$R2G_DB_PASSWORD" in resolved.connection_string
        assert resolved.notes

    def test_search_returns_list(self):
        with _provider() as p:
            results = p.search("a", limit=5)
        assert isinstance(results, list)
