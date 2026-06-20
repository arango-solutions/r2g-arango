"""Unit tests for the OpenMetadata provider.

The HTTP layer is replaced by a fake ``_get`` returning canned OpenMetadata API
payloads, so these run with no network and no httpx server — the same
mocked-driver approach used for the database connectors.
"""
from __future__ import annotations

from r2g.catalogs.base import create_catalog_provider
from r2g.catalogs.openmetadata import OpenMetadataProvider, _api_base, _split_host_port

# ── Canned OpenMetadata API responses ────────────────────────────────────

_PG_SERVICE = {
    "name": "pg",
    "fullyQualifiedName": "pg",
    "serviceType": "Postgres",
    "connection": {"config": {"hostPort": "warehouse.db:5432", "database": "shop"}},
}
_KAFKA_SERVICE = {
    "name": "events",
    "fullyQualifiedName": "events",
    "serviceType": "Kafka",
    "connection": {"config": {"bootstrapServers": "broker1:9092,broker2:9092"}},
}


def _fake_get(path, params=None):
    params = params or {}
    if path == "/services/databaseServices":
        return {"data": [_PG_SERVICE]}
    if path == "/services/messagingServices":
        return {"data": [_KAFKA_SERVICE]}
    if path == "/services/databaseServices/name/pg":
        return _PG_SERVICE
    if path == "/services/messagingServices/name/events":
        return _KAFKA_SERVICE
    if path == "/databases":
        return {"data": [{"name": "shop", "fullyQualifiedName": "pg.shop"}]}
    if path == "/databaseSchemas":
        return {"data": [{"name": "public", "fullyQualifiedName": "pg.shop.public"}]}
    if path == "/tables":
        return {
            "data": [
                {"name": "users", "fullyQualifiedName": "pg.shop.public.users"},
                {"name": "orders", "fullyQualifiedName": "pg.shop.public.orders"},
            ]
        }
    if path == "/topics":
        return {"data": [{"name": "orders.cdc", "fullyQualifiedName": "events.orders.cdc"}]}
    if path == "/databases/name/pg.shop":
        return {
            "name": "shop", "fullyQualifiedName": "pg.shop", "serviceType": "Postgres",
            "service": {"name": "pg", "type": "databaseService"},
        }
    if path == "/databaseSchemas/name/pg.shop.public":
        return {
            "name": "public", "fullyQualifiedName": "pg.shop.public", "serviceType": "Postgres",
            "service": {"name": "pg"}, "database": {"name": "shop"},
        }
    if path == "/tables/name/pg.shop.public.users":
        return {
            "name": "users", "fullyQualifiedName": "pg.shop.public.users", "serviceType": "Postgres",
            "service": {"name": "pg"}, "database": {"name": "shop"},
            "databaseSchema": {"name": "public"},
        }
    if path == "/search/query":
        return {
            "hits": {
                "hits": [
                    {"_source": {
                        "name": "users", "fullyQualifiedName": "pg.shop.public.users",
                        "serviceType": "Postgres",
                        "service": {"name": "pg"}, "database": {"name": "shop"},
                        "databaseSchema": {"name": "public"},
                    }}
                ]
            }
        }
    raise AssertionError(f"unexpected path: {path} {params}")


def _provider() -> OpenMetadataProvider:
    p = create_catalog_provider("openmetadata", "http://localhost:8585", name="om")
    p._get = _fake_get  # type: ignore[method-assign]
    return p


class TestHelpers:
    def test_api_base_normalization(self):
        assert _api_base("http://h:8585") == "http://h:8585/api/v1"
        assert _api_base("http://h:8585/") == "http://h:8585/api/v1"
        assert _api_base("http://h:8585/api") == "http://h:8585/api/v1"
        assert _api_base("http://h:8585/api/v1/") == "http://h:8585/api/v1"

    def test_split_host_port(self):
        assert _split_host_port("db:5432", 5432) == ("db", 5432)
        assert _split_host_port("justhost", 3306) == ("justhost", 3306)
        assert _split_host_port("", 5432) == ("", 5432)


class TestDiscovery:
    def test_list_data_sources_maps_service_types(self):
        assets = _provider().list_data_sources()
        by_name = {a.name: a for a in assets}
        assert by_name["pg"].source_type == "postgresql"
        assert by_name["pg"].kind == "service"
        assert by_name["pg"].connection_hint["hostPort"] == "warehouse.db:5432"
        assert by_name["events"].source_type == "kafka"
        assert by_name["events"].connection_hint["bootstrapServers"].startswith("broker1")

    def test_descend_service_to_tables(self):
        p = _provider()
        svc = next(a for a in p.list_data_sources() if a.name == "pg")
        dbs = p.list_children(svc)
        assert [d.name for d in dbs] == ["shop"]
        assert dbs[0].kind == "database"

        schemas = p.list_children(dbs[0])
        assert [s.name for s in schemas] == ["public"]
        assert schemas[0].kind == "schema"

        tables = p.list_children(schemas[0])
        assert {t.name for t in tables} == {"users", "orders"}
        assert all(t.kind == "table" and t.source_type == "postgresql" for t in tables)

    def test_kafka_service_lists_topics(self):
        p = _provider()
        kafka = next(a for a in p.list_data_sources() if a.name == "events")
        topics = p.list_children(kafka)
        assert [t.name for t in topics] == ["orders.cdc"]
        assert topics[0].kind == "topic"

    def test_search_returns_tables(self):
        assets = _provider().search("user")
        assert assets[0].name == "users"
        assert assets[0].source_type == "postgresql"
        assert assets[0].connection_hint["database"] == "shop"


class TestResolveSource:
    def test_resolve_database_to_postgres_without_secrets(self):
        p = _provider()
        svc = next(a for a in p.list_data_sources() if a.name == "pg")
        db = p.list_children(svc)[0]
        resolved = p.resolve_source(db)
        assert resolved.source_type == "postgresql"
        assert resolved.connection_string == (
            "postgresql://$R2G_DB_USER:$R2G_DB_PASSWORD@warehouse.db:5432/shop"
        )
        # no real credentials leaked into the connection string
        assert "password" not in resolved.connection_string.lower() or "$R2G_DB_PASSWORD" in resolved.connection_string
        assert resolved.notes

    def test_resolve_schema_sets_schema_name(self):
        p = _provider()
        asset = p.get_asset("pg.shop.public")
        assert asset is not None and asset.kind == "schema"
        resolved = p.resolve_source(asset)
        assert resolved.source_type == "postgresql"
        assert resolved.schema_name == "public"
        assert resolved.connection_string.endswith("/shop")

    def test_resolve_kafka_topic(self):
        p = _provider()
        kafka = next(a for a in p.list_data_sources() if a.name == "events")
        topic = p.list_children(kafka)[0]
        resolved = p.resolve_source(topic)
        assert resolved.source_type == "kafka"
        assert resolved.connection_string == "broker1:9092,broker2:9092"
        assert resolved.source_params["topic"] == "orders.cdc"


class TestGetAsset:
    def test_get_asset_infers_kind_by_segment_count(self):
        p = _provider()
        assert p.get_asset("pg.shop").kind == "database"
        assert p.get_asset("pg.shop.public").kind == "schema"
        assert p.get_asset("pg.shop.public.users").kind == "table"

    def test_table_asset_carries_db_and_schema_hint(self):
        p = _provider()
        t = p.get_asset("pg.shop.public.users")
        assert t.source_type == "postgresql"
        assert t.connection_hint["database"] == "shop"
        assert t.connection_hint["schema"] == "public"
