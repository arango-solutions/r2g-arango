"""Tests for the forward CSI v1 emitter (src/r2g/csi.py)."""

from __future__ import annotations

import sys

import pytest

from r2g.csi import CSI_VERSION, csi_schema, mapping_to_csi, validate_csi
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    FieldExpression,
    MappingConfig,
    Schema,
    Table,
)


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Restore structlog to real stderr after a CliRunner invocation.

    CliRunner redirects stdout/stderr; structlog caches the (now-closed) stream,
    which poisons logging in every later test. Mirrors the fixture in
    tests/test_cli.py.
    """
    yield
    import structlog

    structlog.reset_defaults()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


def _sample_config() -> MappingConfig:
    """A two-table (users, orders) + one FK-edge mapping."""
    return MappingConfig(
        source_schema="shop",
        collections={
            "users": CollectionMapping(
                source_table="users",
                target_collection="User",
                field_mappings={"full_name": "name"},
            ),
            "orders": CollectionMapping(
                source_table="orders",
                target_collection="Order",
                field_expressions=[
                    FieldExpression(target="total_cents", sources=["total"], expression="total * 100"),
                ],
            ),
            # A join table -> becomes a relationship, not an entity.
            "user_orders": CollectionMapping(
                source_table="user_orders",
                target_collection="placed",
                collection_type="edge",
                is_join_table=True,
            ),
        },
        edges=[
            EdgeDefinition(
                edge_collection="placed_by",
                from_collection="orders",  # source-table name
                to_collection="users",  # source-table name
                from_fields=["user_id"],
                to_fields=["id"],
            ),
        ],
    )


def _sample_schema() -> Schema:
    return Schema(
        tables={
            "users": Table(
                name="users",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="full_name", data_type="text"),
                    Column(name="email", data_type="text"),
                ],
                primary_key=["id"],
            ),
            "orders": Table(
                name="orders",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="user_id", data_type="integer"),
                    Column(name="total", data_type="numeric"),
                ],
                primary_key=["id"],
            ),
        }
    )


def test_emits_valid_csi_without_schema():
    doc = mapping_to_csi(_sample_config(), source_type="postgresql")
    validate_csi(doc)  # raises if invalid
    assert doc["csiVersion"] == CSI_VERSION == "1"


def test_emits_valid_csi_with_schema():
    doc = mapping_to_csi(_sample_config(), _sample_schema(), source_type="postgresql")
    validate_csi(doc)


def test_entities_are_document_collections_only():
    doc = mapping_to_csi(_sample_config())
    names = {e["name"] for e in doc["conceptualModel"]["entities"]}
    # Join table 'placed' must NOT appear as an entity.
    assert names == {"User", "Order"}
    assert set(doc["arangoPhysicalMapping"]["entities"]) == {"User", "Order"}
    for phys in doc["arangoPhysicalMapping"]["entities"].values():
        assert phys["style"] == "COLLECTION"


def test_relationship_endpoints_resolve_to_target_collections():
    doc = mapping_to_csi(_sample_config())
    rels = doc["conceptualModel"]["relationships"]
    assert len(rels) == 1
    rel = rels[0]
    assert rel["type"] == "placed_by"
    # from_collection='orders' -> 'Order', to_collection='users' -> 'User'.
    assert rel["fromEntity"] == "Order"
    assert rel["toEntity"] == "User"


def test_physical_relationships_omit_collection_name():
    doc = mapping_to_csi(_sample_config())
    phys = doc["arangoPhysicalMapping"]["relationships"]["placed_by"]
    assert phys["style"] == "DEDICATED_COLLECTION"
    assert phys["edgeCollectionName"] == "placed_by"
    # CSI schema forbids collectionName on relationships.
    assert "collectionName" not in phys


def test_properties_prefer_mapping_then_columns():
    doc = mapping_to_csi(_sample_config(), _sample_schema())
    entities = {e["name"]: e for e in doc["conceptualModel"]["entities"]}
    user_props = [p["name"] for p in entities["User"]["properties"]]
    # Renamed 'full_name' -> 'name' comes first; unmapped columns follow (as-is).
    assert user_props[0] == "name"
    assert "email" in user_props
    assert "id" in user_props
    # The renamed source column 'full_name' must not leak through as itself.
    assert "full_name" not in user_props


def test_properties_without_schema_use_explicit_mappings_only():
    doc = mapping_to_csi(_sample_config())
    entities = {e["name"]: e for e in doc["conceptualModel"]["entities"]}
    assert [p["name"] for p in entities["User"]["properties"]] == ["name"]
    assert [p["name"] for p in entities["Order"]["properties"]] == ["total_cents"]


def test_provenance_shape():
    doc = mapping_to_csi(
        _sample_config(),
        source_type="mysql",
        source_ref="shopdb",
        producer_version="9.9.9",
        generated_at="2026-07-14T00:00:00+00:00",
        confidence=1.0,
    )
    prov = doc["provenance"]
    assert prov["producer"] == "r2g"
    assert prov["producerVersion"] == "9.9.9"
    assert prov["direction"] == "forward"
    assert prov["source"] == {"kind": "mysql", "ref": "shopdb", "fingerprint": None}
    assert prov["generatedAt"] == "2026-07-14T00:00:00+00:00"
    assert prov["confidence"] == 1.0


def test_source_ref_defaults_to_source_schema():
    doc = mapping_to_csi(_sample_config())
    assert doc["provenance"]["source"]["ref"] == "shop"
    assert doc["provenance"]["source"]["kind"] == "relational"


def test_producer_version_defaults_to_installed():
    from r2g import __version__

    doc = mapping_to_csi(_sample_config())
    assert doc["provenance"]["producerVersion"] == __version__


def test_confidence_omitted_by_default():
    doc = mapping_to_csi(_sample_config())
    assert "confidence" not in doc["provenance"]


def test_emitter_is_deterministic():
    cfg = _sample_config()
    assert mapping_to_csi(cfg) == mapping_to_csi(cfg)


def test_csi_schema_loads():
    schema = csi_schema()
    assert schema["required"] == [
        "csiVersion",
        "conceptualModel",
        "arangoPhysicalMapping",
        "provenance",
    ]


def test_invalid_document_rejected():
    import jsonschema

    with pytest.raises(jsonschema.ValidationError):
        validate_csi({"csiVersion": "1"})  # missing required blocks


def test_export_csi_cli(tmp_path):
    import json

    from typer.testing import CliRunner

    from r2g.main import app

    config_path = tmp_path / "mapping.yaml"
    config_path.write_text(
        "source_schema: shop\n"
        "collections:\n"
        "  users:\n"
        "    source_table: users\n"
        "    target_collection: User\n"
        "    field_mappings:\n"
        "      full_name: name\n"
        "  orders:\n"
        "    source_table: orders\n"
        "    target_collection: Order\n"
        "edges:\n"
        "  - edge_collection: placed_by\n"
        "    from_collection: orders\n"
        "    to_collection: users\n"
        "    from_field: user_id\n"
        "    to_field: id\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.csi.json"
    result = CliRunner().invoke(
        app,
        ["export-csi", "--config", str(config_path), "--source-type", "postgresql", "-o", str(out)],
    )
    assert result.exit_code == 0, result.output
    doc = json.loads(out.read_text(encoding="utf-8"))
    validate_csi(doc)
    assert doc["provenance"]["source"]["kind"] == "postgresql"
    assert {e["name"] for e in doc["conceptualModel"]["entities"]} == {"User", "Order"}
