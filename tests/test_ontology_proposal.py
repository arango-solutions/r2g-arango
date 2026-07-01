"""Unit tests for proposal_to_mapping — the hallucination gate (Phase 10a)."""
from __future__ import annotations

from r2g.config import validate_config
from r2g.llm.base import (
    OntologyProposal,
    ProposedCollection,
    ProposedEdge,
    ProposedEmbed,
    ProposedRename,
)
from r2g.llm.ontology import proposal_to_mapping
from r2g.types import Column, ForeignKey, Schema, Table


def _schema(*, with_fk: bool = False) -> Schema:
    orders_fk = (
        [ForeignKey(columns=["customer_id"], foreign_table="customer", foreign_columns=["id"])]
        if with_fk
        else []
    )
    return Schema(
        tables={
            "customer": Table(
                name="customer",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="email", data_type="text"),
                ],
                primary_key=["id"],
            ),
            "orders": Table(
                name="orders",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="customer_id", data_type="integer"),
                ],
                primary_key=["id"],
                foreign_keys=orders_fk,
            ),
        }
    )


class TestBaselineAndValidity:
    def test_empty_proposal_is_automap_baseline(self):
        schema = _schema()
        config, notes = proposal_to_mapping(OntologyProposal(), schema)
        assert set(config.collections) == {"customer", "orders"}
        assert validate_config(schema, config) == []

    def test_result_always_validates(self):
        schema = _schema()
        # A maximally hostile proposal: hallucinated everything.
        proposal = OntologyProposal(
            collections=[ProposedCollection(source_table="ghost", target_collection="Ghost")],
            edges=[
                ProposedEdge(
                    edge_collection="bad",
                    from_collection="ghost",
                    to_collection="customer",
                    from_fields=["nope"],
                    to_fields=["id"],
                )
            ],
            renames=[ProposedRename(source_table="ghost", column="x", target_property="y")],
        )
        config, notes = proposal_to_mapping(proposal, schema)
        assert validate_config(schema, config) == []


class TestHallucinationDropping:
    def test_unknown_table_collection_dropped(self):
        schema = _schema()
        proposal = OntologyProposal(
            collections=[ProposedCollection(source_table="ghost", target_collection="Ghost")]
        )
        config, notes = proposal_to_mapping(proposal, schema)
        assert "ghost" not in config.collections
        assert any("ghost" in n and "hallucinated" in n for n in notes)

    def test_unknown_edge_column_dropped(self):
        schema = _schema()
        proposal = OntologyProposal(
            edges=[
                ProposedEdge(
                    edge_collection="o_to_c",
                    from_collection="orders",
                    to_collection="customer",
                    from_fields=["does_not_exist"],
                    to_fields=["id"],
                )
            ]
        )
        config, notes = proposal_to_mapping(proposal, schema)
        assert config.edges == []
        assert any("does_not_exist" in n or "unknown join column" in n for n in notes)

    def test_reserved_rename_target_dropped(self):
        schema = _schema()
        proposal = OntologyProposal(
            renames=[ProposedRename(source_table="customer", column="id", target_property="_key")]
        )
        config, notes = proposal_to_mapping(proposal, schema)
        assert config.collections["customer"].field_mappings == {}
        assert any("reserved" in n for n in notes)


class TestEnrichment:
    def test_implicit_edge_added(self):
        schema = _schema(with_fk=False)  # no declared FK
        proposal = OntologyProposal(
            edges=[
                ProposedEdge(
                    edge_collection="orders_to_customer",
                    from_collection="orders",
                    to_collection="customer",
                    from_fields=["customer_id"],
                    to_fields=["id"],
                    rationale="customer_id clearly references customer.id",
                )
            ]
        )
        config, notes = proposal_to_mapping(proposal, schema)
        assert any(e.edge_collection == "orders_to_customer" for e in config.edges)
        assert validate_config(schema, config) == []

    def test_declared_fk_not_duplicated(self):
        schema = _schema(with_fk=True)  # baseline already has the edge
        proposal = OntologyProposal(
            edges=[
                ProposedEdge(
                    edge_collection="restated",
                    from_collection="orders",
                    to_collection="customer",
                    from_fields=["customer_id"],
                    to_fields=["id"],
                )
            ]
        )
        config, notes = proposal_to_mapping(proposal, schema)
        # Same (from,to,fields) signature as the baseline FK edge → not added again.
        sigs = [(e.from_collection, e.to_collection, tuple(e.from_fields)) for e in config.edges]
        assert sigs.count(("orders", "customer", ("customer_id",))) == 1

    def test_rename_applied(self):
        schema = _schema()
        proposal = OntologyProposal(
            renames=[
                ProposedRename(
                    source_table="customer", column="email", target_property="email_address"
                )
            ]
        )
        config, notes = proposal_to_mapping(proposal, schema)
        assert config.collections["customer"].field_mappings["email"] == "email_address"

    def test_collection_type_and_join_flag_applied(self):
        schema = _schema()
        proposal = OntologyProposal(
            collections=[
                ProposedCollection(
                    source_table="orders",
                    target_collection="Order",
                    collection_type="document",
                    is_join_table=True,
                )
            ]
        )
        config, notes = proposal_to_mapping(proposal, schema)
        assert config.collections["orders"].target_collection == "Order"
        assert config.collections["orders"].is_join_table is True

    def test_embed_is_advisory_note_only(self):
        schema = _schema()
        proposal = OntologyProposal(
            embeds=[
                ProposedEmbed(
                    parent_table="customer", child_table="orders", as_property="orders"
                )
            ]
        )
        config, notes = proposal_to_mapping(proposal, schema)
        # Embeds never mutate the mapping in V1; they only surface as notes.
        assert any("Embed hint" in n for n in notes)
        assert config.collections["orders"].collection_type == "document"


class TestDeterminism:
    def test_same_proposal_same_mapping(self):
        schema = _schema()
        proposal = OntologyProposal(
            edges=[
                ProposedEdge(
                    edge_collection="orders_to_customer",
                    from_collection="orders",
                    to_collection="customer",
                    from_fields=["customer_id"],
                    to_fields=["id"],
                )
            ]
        )
        c1, _ = proposal_to_mapping(proposal, schema)
        c2, _ = proposal_to_mapping(proposal, schema)
        assert c1.model_dump() == c2.model_dump()
