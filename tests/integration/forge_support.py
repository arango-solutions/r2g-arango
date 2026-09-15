"""Shared fixture data and the ``≡`` comparison for the Federation Forge
roundtrip tests (ADR-0006 D-3), used by every dialect's live test.

``introspect(generate(O)) ≡ O`` is equality *up to* CC-12 naming normalization
and declared-constraint availability (PLAN F-2/F-3): entity and property names
compare through the real ``owl_entity_name`` / ``owl_property_name``
normalizers, relationship types through ``owl_property_name``, and the only
tolerated extras are the generated surrogate plumbing (the ``id`` PK plus one
FK label per outgoing relationship). Anything else unexpected fails.
"""

from __future__ import annotations

from typing import Dict, Iterable, Mapping, Set, Tuple

from r2g.csi import owl_property_name
from r2g.forge import ForgeOntology, foreign_key_column, table_name

SEED = 421
ROWS_PER_ENTITY = 20

#: Three entities, a two-level relationship chain, all four JSON types, and a
#: multi-word class (``SupportTicket``) so pluralize/singularize and camel-case
#: conversion are exercised in every dialect's spelling.
CONCEPTUAL = {
    "entities": [
        {
            "name": "Account",
            "properties": [
                {"name": "accountName", "type": "string"},
                {"name": "healthScore", "type": "float"},
                {"name": "seatsSold", "type": "integer"},
            ],
        },
        {
            "name": "Contact",
            "properties": [
                {"name": "fullName", "type": "string"},
                {"name": "isPrimary", "type": "boolean"},
            ],
        },
        {
            "name": "SupportTicket",
            "properties": [
                {"name": "severity", "type": "integer"},
                {"name": "resolved", "type": "boolean"},
            ],
        },
    ],
    "relationships": [
        {"type": "contactsToAccounts", "fromEntity": "Contact", "toEntity": "Account"},
        {"type": "supportTicketsToContacts", "fromEntity": "SupportTicket", "toEntity": "Contact"},
    ],
}


def forge_ontology() -> ForgeOntology:
    return ForgeOntology.from_conceptual(CONCEPTUAL)


def plumbing_labels(ontology: ForgeOntology, entity_name: str) -> Set[str]:
    """The generated surrogate columns the forward pipeline reports as extra
    conceptual properties: the ``id`` PK plus one FK label per outgoing
    relationship (``account_id`` -> ``accountId``)."""
    labels = {"id"}
    for r in ontology.relationships:
        if r.from_entity == entity_name:
            labels.add(owl_property_name(foreign_key_column(r.to_entity)))
    return labels


def all_plumbing_labels(ontology: ForgeOntology) -> Set[str]:
    return set().union(*(plumbing_labels(ontology, e.name) for e in ontology.entities))


def expected_tables(ontology: ForgeOntology, physical=lambda t: t) -> Set[str]:
    """Canonical table names projected through a dialect's spelling."""
    return {physical(table_name(e.name)) for e in ontology.entities}


def expected_fk_pairs(ontology: ForgeOntology, physical=lambda t: t) -> Set[Tuple[str, str]]:
    """``(from_table, to_table)`` per relationship, in the dialect's spelling."""
    return {
        (physical(table_name(r.from_entity)), physical(table_name(r.to_entity)))
        for r in ontology.relationships
    }


def normalized_entities(entities: Iterable[Mapping]) -> Dict[str, Set[str]]:
    """``{EntityName: {lowerCamel property, ...}}`` from a conceptual model,
    normalizing physical property spellings (``account_name``,
    ``ACCOUNT_NAME``) through the real CC-12 property normalizer. Entity names
    already come normalized from every analyzer (``owl_entity_name`` or ASA's
    ``pascal_case(singularize(...))``)."""
    return {
        e["name"]: {owl_property_name(p["name"]) for p in e.get("properties", [])}
        for e in entities
    }


def normalized_relationships(relationships: Iterable[Mapping]) -> Set[Tuple[str, str, str]]:
    """``(type, fromEntity, toEntity)`` with the type normalized through
    ``owl_property_name`` (ASA reports ``CONTACTS_TO_ACCOUNTS``; r2g's CSI
    emitter already lowerCamels it)."""
    return {
        (owl_property_name(r["type"]), r["fromEntity"], r["toEntity"]) for r in relationships
    }


def assert_conceptual_model_matches(
    ontology: ForgeOntology,
    entities: Iterable[Mapping],
    relationships: Iterable[Mapping],
) -> None:
    """The ``≡`` of ADR-0006 D-3 over a conceptual model (entities with
    properties, relationships with endpoints)."""
    got_entities = normalized_entities(entities)
    assert set(got_entities) == {e.name for e in ontology.entities}, (
        f"entities differ: got {sorted(got_entities)}"
    )
    for entity in ontology.entities:
        declared = {p.name for p in entity.properties}
        got = got_entities[entity.name]
        missing = declared - got
        assert not missing, f"{entity.name}: properties lost in roundtrip: {missing}"
        extras = got - declared - plumbing_labels(ontology, entity.name)
        assert not extras, f"{entity.name}: unexpected extra properties: {extras}"

    got_relationships = normalized_relationships(relationships)
    expected = {(r.type, r.from_entity, r.to_entity) for r in ontology.relationships}
    assert got_relationships == expected, f"relationships differ: got {sorted(got_relationships)}"


def assert_business_collisions_are_plumbing_only(ontology: ForgeOntology, csi: Mapping) -> None:
    """F-6: business labels stayed collision-free by construction; the only
    collisions the CSI emitter may record are the generated plumbing."""
    plumbing = all_plumbing_labels(ontology)
    collision_labels = {c["label"] for c in csi["provenance"].get("labelCollisions", [])}
    assert collision_labels <= plumbing, f"business-label collisions: {collision_labels - plumbing}"
