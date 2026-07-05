"""Unit tests for the relational-schema-analyzer (RSA) ontology adapter.

The ``bundle_to_proposal`` tests use synthetic tool-contract bundles and never
import RSA, so they always run. The end-to-end tests that drive the real
analyzer are guarded by ``importorskip``.
"""
from __future__ import annotations

import pytest

from r2g.config import validate_config
from r2g.llm.ontology import proposal_to_mapping
from r2g.rsa_ontology import bundle_to_proposal, propose_ontology_from_schema
from r2g.types import Column, ForeignKey, Schema, Table


def _bundle(entities=None, relationships=None, metadata=None) -> dict:
    return {
        "conceptualSchema": {"entities": [], "relationships": [], "properties": []},
        "physicalMapping": {
            "entities": entities or {},
            "relationships": relationships or {},
        },
        "metadata": metadata or {},
    }


class TestBundleToProposal:
    def test_entities_become_collection_renames(self):
        bundle = _bundle(
            entities={
                "Orders": {
                    "style": "TABLE",
                    "tableName": "orders",
                    "primaryKey": ["id"],
                    "properties": {
                        "id": {"field": "id"},
                        "total": {"field": "total"},
                    },
                }
            },
            metadata={"confidence": 0.9},
        )
        proposal = bundle_to_proposal(bundle)
        assert len(proposal.collections) == 1
        pc = proposal.collections[0]
        assert pc.source_table == "orders"
        assert pc.target_collection == "Orders"
        assert pc.confidence == 0.9
        # Baseline keeps property names verbatim → no renames.
        assert proposal.renames == []

    def test_property_conceptual_name_becomes_rename(self):
        bundle = _bundle(
            entities={
                "Customer": {
                    "style": "TABLE",
                    "tableName": "customer",
                    "properties": {
                        "emailAddress": {"field": "email_addr"},
                        "id": {"field": "id"},
                    },
                }
            }
        )
        proposal = bundle_to_proposal(bundle)
        assert len(proposal.renames) == 1
        rn = proposal.renames[0]
        assert (rn.source_table, rn.column, rn.target_property) == (
            "customer",
            "email_addr",
            "emailAddress",
        )

    def test_foreign_key_relationship_becomes_edge(self):
        bundle = _bundle(
            relationships={
                "Orders_Customer": {
                    "style": "FOREIGN_KEY",
                    "fromTable": "orders",
                    "fromColumns": ["customer_id"],
                    "toTable": "customer",
                    "toColumns": ["id"],
                }
            }
        )
        proposal = bundle_to_proposal(bundle)
        assert len(proposal.edges) == 1
        e = proposal.edges[0]
        assert e.from_collection == "orders"
        assert e.to_collection == "customer"
        assert e.from_fields == ["customer_id"]
        assert e.to_fields == ["id"]

    def test_join_table_flags_collection_and_notes_m2m(self):
        bundle = _bundle(
            entities={
                "Orders": {"style": "TABLE", "tableName": "orders", "properties": {}},
            },
            relationships={
                "Orders_Products": {
                    "style": "JOIN_TABLE",
                    "joinTable": "order_items",
                    "joinFromColumns": ["order_id"],
                    "joinToColumns": ["product_id"],
                    "attributeColumns": ["qty"],
                }
            },
        )
        proposal = bundle_to_proposal(bundle)
        # order_items is not an entity, so it is added explicitly as a join table.
        jt = [c for c in proposal.collections if c.source_table == "order_items"]
        assert jt and jt[0].is_join_table is True
        assert any("Many-to-many" in n and "order_items" in n for n in proposal.notes)
        assert any("qty" in n for n in proposal.notes)

    def test_join_table_that_is_also_entity_gets_flag(self):
        bundle = _bundle(
            entities={
                "OrderItems": {"style": "TABLE", "tableName": "order_items", "properties": {}},
            },
            relationships={
                "R": {
                    "style": "JOIN_TABLE",
                    "joinTable": "order_items",
                    "joinFromColumns": ["a"],
                    "joinToColumns": ["b"],
                }
            },
        )
        proposal = bundle_to_proposal(bundle)
        items = [c for c in proposal.collections if c.source_table == "order_items"]
        # Not duplicated, and flagged.
        assert len(items) == 1
        assert items[0].is_join_table is True

    def test_metadata_surfaces_as_notes(self):
        bundle = _bundle(
            metadata={
                "detectedPatterns": ["join_table"],
                "assumptions": ["assumed X"],
                "reviewRequired": True,
            }
        )
        proposal = bundle_to_proposal(bundle)
        joined = " | ".join(proposal.notes)
        assert "join_table" in joined
        assert "assumed X" in joined
        assert "manual review" in joined

    def test_defensive_against_garbage_shapes(self):
        # Non-dict members and missing keys must not raise.
        bundle = {
            "physicalMapping": {
                "entities": {"Bad": "notadict", "Ok": {"tableName": "ok", "properties": None}},
                "relationships": {"R": "notadict", "F": {"style": "FOREIGN_KEY"}},
            },
            "metadata": {"confidence": "bogus"},
        }
        proposal = bundle_to_proposal(bundle)
        assert [c.source_table for c in proposal.collections] == ["ok"]
        # FK with missing endpoints is dropped.
        assert proposal.edges == []

    def test_confidence_out_of_range_falls_back(self):
        bundle = _bundle(
            entities={"E": {"tableName": "t", "properties": {}}},
            metadata={"confidence": 5.0},
        )
        proposal = bundle_to_proposal(bundle)
        assert proposal.collections[0].confidence == pytest.approx(0.9)


class TestRealAnalyzerEndToEnd:
    """Drive the actual RSA library (skips when not installed)."""

    def _schema(self) -> Schema:
        return Schema(
            tables={
                "customer": Table(
                    name="customer",
                    columns=[
                        Column(name="id", data_type="int", is_primary_key=True),
                        Column(name="name", data_type="text"),
                    ],
                    primary_key=["id"],
                ),
                "orders": Table(
                    name="orders",
                    columns=[
                        Column(name="id", data_type="int", is_primary_key=True),
                        Column(name="customer_id", data_type="int"),
                    ],
                    primary_key=["id"],
                    foreign_keys=[
                        ForeignKey(
                            column="customer_id",
                            foreign_table="customer",
                            foreign_column="id",
                        )
                    ],
                ),
                "order_items": Table(
                    name="order_items",
                    columns=[
                        Column(name="order_id", data_type="int"),
                        Column(name="product_id", data_type="int"),
                    ],
                    primary_key=["order_id", "product_id"],
                    foreign_keys=[
                        ForeignKey(column="order_id", foreign_table="orders", foreign_column="id"),
                        ForeignKey(
                            column="product_id", foreign_table="products", foreign_column="id"
                        ),
                    ],
                ),
                "products": Table(
                    name="products",
                    columns=[Column(name="id", data_type="int", is_primary_key=True)],
                    primary_key=["id"],
                ),
            }
        )

    def test_deterministic_proposal_and_valid_mapping(self):
        pytest.importorskip("relational_schema_analyzer")
        schema = self._schema()
        proposal, meta = propose_ontology_from_schema(schema)

        renames = {c.source_table: c.target_collection for c in proposal.collections}
        assert renames.get("orders") == "Orders"
        assert renames.get("customer") == "Customer"
        # Join table detected and flagged.
        assert any(
            c.source_table == "order_items" and c.is_join_table for c in proposal.collections
        )
        assert meta.get("confidence") is not None

        # The proposal always yields a schema-valid, loadable mapping.
        config, _notes = proposal_to_mapping(proposal, schema)
        assert validate_config(schema, config) == []
        assert config.collections["orders"].target_collection == "Orders"
