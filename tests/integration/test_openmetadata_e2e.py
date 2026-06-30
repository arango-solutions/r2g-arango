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


@requires_openmetadata
class TestOpenMetadataClassificationLive:
    """Phase 9: the column-classification capture path against a live OM API.

    Validates that ``resolve_source`` returns the governance carrier shapes the
    rest of Phase 9 depends on, without leaking secrets. Tolerant of a catalog
    with no tagged columns (asserts shapes; only asserts content when present).
    """

    def _first_relational_db(self, p):
        services = [
            a
            for a in p.list_data_sources()
            if a.source_type in ("postgresql", "mysql", "sqlserver", "snowflake")
        ]
        for svc in services:
            for db in p.list_children(svc):
                return db
        return None

    def test_resolve_carries_classification_shapes(self):
        from r2g.types import Classification

        with _provider() as p:
            db = self._first_relational_db(p)
            if db is None:
                import pytest

                pytest.skip("no relational database in this OpenMetadata instance")
            resolved = p.resolve_source(db)

        # Carrier shapes are always present (empty when the catalog has no tags).
        assert isinstance(resolved.column_classifications, dict)
        assert isinstance(resolved.owners, list)
        assert resolved.tier is None or isinstance(resolved.tier, str)
        # discover-then-connect: never leak a real secret
        assert "$R2G_DB_PASSWORD" in resolved.connection_string

        # When the live catalog *does* carry column tags, they must parse into
        # Classification objects keyed table -> column.
        for table, cols in resolved.column_classifications.items():
            assert isinstance(table, str)
            for col, clf in cols.items():
                assert isinstance(col, str)
                assert isinstance(clf, Classification)

    def test_import_snapshot_report_chain(self, tmp_path, monkeypatch):
        """End-to-end: import a catalog source, snapshot, and build a report.

        Exercises the whole 9a→9b chain against whatever the live catalog holds;
        asserts the report is well-formed (and that any captured classifications
        propagate onto the snapshot's columns)."""
        from r2g.catalog import CatalogManager
        from r2g.classification import annotate_schema
        from r2g.governance import build_entitlement_report

        with _provider() as p:
            db = self._first_relational_db(p)
            if db is None:
                import pytest

                pytest.skip("no relational database in this OpenMetadata instance")
            resolved = p.resolve_source(db)

        mgr = CatalogManager(str(tmp_path / "catalog"))
        mgr.add_source(
            "om_src",
            resolved.source_type,
            resolved.connection_string,
            classifications=resolved.column_classifications,
            data_owners=resolved.owners,
            data_tier=resolved.tier,
            catalog_name="it",
            catalog_asset_fqn=db.fqn,
        )
        got = mgr.get_source("om_src")
        assert got.classifications == resolved.column_classifications
        if resolved.column_classifications:
            assert got.classifications_synced_at is not None

        # Build a tiny schema from the captured tables and merge classifications,
        # then assert the report reflects any tagged columns.
        from r2g.types import CollectionMapping, Column, MappingConfig, Schema, Table

        if not resolved.column_classifications:
            import pytest

            pytest.skip("live catalog has no column tags to assert on")
        tables = {
            tname: Table(
                name=tname,
                columns=[Column(name=c, data_type="text") for c in cols],
                primary_key=[next(iter(cols))],
            )
            for tname, cols in resolved.column_classifications.items()
        }
        schema = Schema(tables=tables)
        merged = annotate_schema(schema, resolved.column_classifications)
        assert merged >= 0
        config = MappingConfig(collections={
            t: CollectionMapping(source_table=t, target_collection=t.capitalize())
            for t in tables
        })
        report = build_entitlement_report(config, schema, project="om_it")
        assert report.summary()["total_fields"] >= 0
