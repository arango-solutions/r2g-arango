"""Tests for external-catalog registration in CatalogManager (Phase 8)."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from r2g.catalog import CatalogManager


@pytest.fixture(autouse=True)
def _silence_catalog_logger():
    """CatalogManager logs via structlog on add/remove; under the full suite
    that logger can be left bound to a closed stream by other CLI tests. These
    tests don't assert on logs, so neutralize it."""
    with patch("r2g.catalog.logger", MagicMock()):
        yield


def _mgr(tmp_path) -> CatalogManager:
    return CatalogManager(str(tmp_path))


class TestCatalogRegistry:
    def test_add_and_get(self, tmp_path):
        mgr = _mgr(tmp_path)
        cfg = mgr.add_catalog(
            "corp", "openmetadata", "http://localhost:8585", token="secret-tok", description="Corp OM"
        )
        assert cfg.provider_type == "openmetadata"
        got = mgr.get_catalog("corp")
        assert got is not None
        assert got.endpoint == "http://localhost:8585"
        assert got.token == "secret-tok"  # decrypted on read

    def test_alias_normalized_on_add(self, tmp_path):
        cfg = _mgr(tmp_path).add_catalog("c", "open-metadata", "http://h:8585")
        assert cfg.provider_type == "openmetadata"

    def test_unsupported_type_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="Unsupported catalog provider type"):
            _mgr(tmp_path).add_catalog("c", "collibra", "http://h")

    def test_duplicate_rejected(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.add_catalog("c", "openmetadata", "http://h:8585")
        with pytest.raises(ValueError, match="already exists"):
            mgr.add_catalog("c", "openmetadata", "http://h:8585")

    def test_list_and_remove(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.add_catalog("a", "openmetadata", "http://a:8585")
        mgr.add_catalog("b", "openmetadata", "http://b:8585")
        assert {c.name for c in mgr.list_catalogs()} == {"a", "b"}
        assert mgr.remove_catalog("a") is True
        assert {c.name for c in mgr.list_catalogs()} == {"b"}
        assert mgr.remove_catalog("missing") is False

    def test_token_encrypted_at_rest(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.add_catalog("c", "openmetadata", "http://h:8585", token="plaintext-token")
        raw = json.loads((tmp_path / "catalog.json").read_text())
        stored = raw["catalog_providers"]["c"]["token"]
        assert stored != "plaintext-token"
        assert stored.startswith("enc:")  # Fernet envelope tag
        # round-trips back to plaintext through a fresh manager
        assert CatalogManager(str(tmp_path)).get_catalog("c").token == "plaintext-token"

    def test_empty_token_not_encrypted(self, tmp_path):
        mgr = _mgr(tmp_path)
        mgr.add_catalog("c", "openmetadata", "http://h:8585")  # no token
        raw = json.loads((tmp_path / "catalog.json").read_text())
        assert raw["catalog_providers"]["c"]["token"] == ""


class TestSourceClassifications:
    """Phase 9a: SourceConfig.classifications round-trips through save/load."""

    def test_classifications_round_trip(self, tmp_path):
        from r2g.types import Classification

        mgr = _mgr(tmp_path)
        mgr.add_source(
            "shop",
            "postgresql",
            "postgresql://h/shop",
            classifications={"customer": {"email": Classification(tags=["PII.Sensitive"])}},
            data_owners=["Data Team"],
            data_tier="Tier.Tier1",
        )
        # fresh manager re-reads from disk
        got = CatalogManager(str(tmp_path)).get_source("shop")
        assert got is not None
        assert got.data_owners == ["Data Team"]
        assert got.data_tier == "Tier.Tier1"
        clf = got.classifications["customer"]["email"]
        assert isinstance(clf, Classification)
        assert clf.tags == ["PII.Sensitive"]

    def test_default_source_has_no_classifications(self, tmp_path):
        mgr = _mgr(tmp_path)
        src = mgr.add_source("plain", "postgresql", "postgresql://h/db")
        assert src.classifications == {}
        assert src.data_owners == []
        assert src.data_tier is None
        assert src.catalog_name is None
        assert src.catalog_asset_fqn is None
        assert src.classifications_synced_at is None

    def test_catalog_provenance_round_trips(self, tmp_path):
        from r2g.types import Classification

        mgr = _mgr(tmp_path)
        src = mgr.add_source(
            "shop",
            "postgresql",
            "postgresql://h/shop",
            classifications={"customer": {"email": Classification(tags=["PII.Sensitive"])}},
            catalog_name="om",
            catalog_asset_fqn="svc.shop.public",
        )
        # importing with classifications stamps a sync timestamp
        assert src.classifications_synced_at is not None
        got = CatalogManager(str(tmp_path)).get_source("shop")
        assert got.catalog_name == "om"
        assert got.catalog_asset_fqn == "svc.shop.public"
        assert got.classifications_synced_at is not None

    def test_update_snapshot_schema(self, tmp_path):
        from r2g.classification import annotate_schema
        from r2g.types import Classification, Column, Schema, Table

        mgr = _mgr(tmp_path)
        mgr.add_source("shop", "postgresql", "postgresql://h/shop")
        schema = Schema(tables={
            "customer": Table(
                name="customer",
                columns=[Column(name="email", data_type="text")],
                primary_key=["email"],
            )
        })
        snap = mgr.create_snapshot("shop", schema)
        annotate_schema(schema, {"customer": {"email": Classification(tags=["PII.Sensitive"])}})
        mgr.update_snapshot_schema(snap.id, schema)
        got = CatalogManager(str(tmp_path)).get_latest_snapshot("shop")
        col = got.schema_data.tables["customer"].columns[0]
        assert col.classification is not None
        assert col.classification.tags == ["PII.Sensitive"]
