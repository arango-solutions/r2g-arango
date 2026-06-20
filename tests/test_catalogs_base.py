from __future__ import annotations

import pytest

from r2g.catalogs.base import (
    SUPPORTED_CATALOG_TYPES,
    CatalogAsset,
    CatalogProvider,
    ResolvedSource,
    create_catalog_provider,
    normalize_catalog_type,
)


class TestNormalization:
    def test_aliases_fold_to_openmetadata(self):
        for alias in ("openmetadata", "OpenMetadata", "open-metadata", "om", "  OM  "):
            assert normalize_catalog_type(alias) == "openmetadata"

    def test_unknown_passthrough(self):
        assert normalize_catalog_type("glue") == "glue"

    def test_supported_types(self):
        assert "openmetadata" in SUPPORTED_CATALOG_TYPES


class TestFactory:
    def test_builds_openmetadata_provider(self):
        from r2g.catalogs.openmetadata import OpenMetadataProvider

        p = create_catalog_provider("openmetadata", "http://localhost:8585", name="om")
        assert isinstance(p, OpenMetadataProvider)
        assert p.name == "om"

    def test_alias_builds_provider(self):
        from r2g.catalogs.openmetadata import OpenMetadataProvider

        assert isinstance(
            create_catalog_provider("open-metadata", "http://h:8585"), OpenMetadataProvider
        )

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported catalog provider type"):
            create_catalog_provider("collibra", "http://h")

    def test_provider_satisfies_protocol(self):
        p = create_catalog_provider("openmetadata", "http://h:8585")
        assert isinstance(p, CatalogProvider)


class TestModels:
    def test_catalog_asset_defaults(self):
        a = CatalogAsset(
            provider="om", provider_type="openmetadata", fqn="pg.shop", kind="database", name="shop"
        )
        assert a.source_type is None
        assert a.connection_hint == {}
        assert a.tags == []

    def test_resolved_source_defaults(self):
        r = ResolvedSource(source_type="postgresql", connection_string="postgresql://h/db")
        assert r.source_params == {}
        assert r.schema_name is None
