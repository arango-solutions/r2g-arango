"""Forward CSI v1 (Conceptual Schema Interchange) emitter.

r2g is the natural *forward* producer of ``CSI v1``: it knows both the source
relational schema *and* the ArangoDB collections it decided to create, so it can
emit a single document that pairs the conceptual model with its Arango physical
mapping. Downstream, the contextual-data-fabric M5 federated-query engine feeds
this bundle to the mapping adapters (CSI->R2RML for the Ontop relational leg,
CSI->MappingBundle for the ``arango-sparql-py`` AQL leg) so a conceptual SPARQL
query can be partitioned by source. See ``docs/PRD.md`` Phase 12 and
contextual-data-fabric ADR-0001 / implementation-plan WP-A1.

The reverse producer is ``arango-schema-analyzer`` (which reads an existing
Arango graph). Both write the *same* ``CSI v1`` contract — this module targets
``schemas/csi_v1.schema.json`` (a vendored copy of the analyzer's authoritative
schema; copy-now-converge-later).

The core entry point :func:`mapping_to_csi` is pure and deterministic (no
timestamps, no I/O) so its output is trivially testable; the CLI layer stamps
``generatedAt``.
"""

from __future__ import annotations

import json
from importlib import resources
from typing import Any, Dict, List, Optional

from .types import (
    CollectionMapping,
    MappingConfig,
    Schema,
    Table,
)

CSI_VERSION = "1"
PRODUCER = "r2g"

# ArangoDB physical styles r2g emits. r2g always maps a table to its own
# document collection and an edge to its own dedicated edge collection, so the
# forward direction only ever produces these two styles (the LABEL /
# GENERIC_WITH_TYPE styles are reverse-direction detections).
_ENTITY_STYLE = "COLLECTION"
_RELATIONSHIP_STYLE = "DEDICATED_COLLECTION"


def owl_entity_name(name: str) -> str:
    """Conceptual entity name per the fabric's CC-12 OWL convention.

    Singular PascalCase (``usage_metrics`` → ``UsageMetric``); the physical
    collection/table name is untouched — it lives in the physical mapping.
    """
    from .naming import convert_identifier, singularize, split_identifier

    words = split_identifier(name)
    if words:
        words[-1] = singularize(words[-1])
    return convert_identifier("_".join(words), "pascal") or name


def owl_property_name(name: str) -> str:
    """Conceptual property/relationship name per CC-12: lowerCamel."""
    from .naming import convert_identifier

    return convert_identifier(name, "camel") or name


def _entity_property_names(cm: CollectionMapping, table: Optional[Table]) -> List[str]:
    """Ordered, de-duplicated target-property names for one collection.

    Prefers the explicit mapping (``field_mappings`` values + ``field_expressions``
    targets); falls back to the source table's columns (honouring
    include/exclude) when the mapping doesn't rename anything.
    """
    names: List[str] = []
    seen: set[str] = set()

    def _add(name: str) -> None:
        if name and name not in seen:
            seen.add(name)
            names.append(name)

    # Explicit field renames: value is the target property name.
    for target in cm.field_mappings.values():
        _add(target)
    # Computed / fan-in properties.
    for fe in cm.field_expressions:
        _add(fe.target)

    # Fall back to the physical columns when we have the schema and no explicit
    # rename covered a column.
    if table is not None:
        include = set(cm.include_fields) if cm.include_fields is not None else None
        exclude = set(cm.exclude_fields)
        for col in table.columns:
            if include is not None and col.name not in include:
                continue
            if col.name in exclude:
                continue
            _add(cm.field_mappings.get(col.name, col.name))

    return names


def mapping_to_csi(
    config: MappingConfig,
    schema: Optional[Schema] = None,
    *,
    source_type: str = "relational",
    source_ref: str = "",
    source_fingerprint: Optional[str] = None,
    producer_version: Optional[str] = None,
    generated_at: Optional[str] = None,
    confidence: Optional[float] = None,
) -> Dict[str, Any]:
    """Emit a forward ``CSI v1`` document from an r2g :class:`MappingConfig`.

    Args:
        config: the r2g mapping (source tables -> collections, FK/join edges).
        schema: optional source :class:`Schema`; when given, entity property
            lists are enriched from the physical columns.
        source_type: ``provenance.source.kind`` (e.g. ``"postgresql"``,
            ``"mysql"``, ``"mssql"``, ``"snowflake"``, ``"csv"``).
        source_ref: ``provenance.source.ref`` — a human pointer to the source
            (database/schema name). Defaults to ``config.source_schema``.
        source_fingerprint: optional content hash of the source schema.
        producer_version: ``provenance.producerVersion``; defaults to the
            installed r2g version.
        generated_at: optional ISO-8601 stamp for ``provenance.generatedAt``
            (kept out of the pure path so callers control determinism).
        confidence: optional 0..1 confidence; r2g's mapping is a deterministic
            mechanical translation, so callers typically leave this unset.

    Returns:
        A ``CSI v1``-valid ``dict`` (validate with :func:`validate_csi`).
    """
    if producer_version is None:
        from . import __version__ as producer_version  # local import: avoid cycle

    tables = schema.tables if schema is not None else {}

    # --- Conceptual + physical entities: one per document collection. ---
    conceptual_entities: List[Dict[str, Any]] = []
    physical_entities: Dict[str, Dict[str, Any]] = {}
    entity_name_by_collection: Dict[str, str] = {}
    for cm in config.collections.values():
        if cm.collection_type == "edge" or cm.is_join_table:
            # Join tables / edge collections are relationships, not entities.
            continue
        # Conceptual name follows CC-12 (singular PascalCase); the physical
        # collection name is preserved in the physical mapping.
        name = owl_entity_name(cm.target_collection)
        entity_name_by_collection[cm.target_collection] = name
        prop_names = _entity_property_names(cm, tables.get(cm.source_table))
        conceptual_entities.append(
            {
                "name": name,
                "labels": [name],
                "properties": [{"name": owl_property_name(p)} for p in prop_names],
            }
        )
        physical_entities[name] = {
            "style": _ENTITY_STYLE,
            "collectionName": cm.target_collection,
            # Conceptual property → stored field, so the AQL/SQL legs resolve
            # OWL-style names back to physical attributes (CC-12).
            "properties": {owl_property_name(p): {"field": p} for p in prop_names},
        }

    # --- Conceptual + physical relationships: one per edge definition. ---
    # EdgeDefinition.from_collection / to_collection reference *source-table*
    # names; resolve them to the collection names that actually hold the data.
    target_by_source = {
        cm.source_table: cm.target_collection for cm in config.collections.values()
    }
    conceptual_relationships: List[Dict[str, Any]] = []
    physical_relationships: Dict[str, Dict[str, Any]] = {}
    for edge in config.edges:
        rel_type = owl_property_name(edge.edge_collection)
        from_coll = target_by_source.get(edge.from_collection, edge.from_collection)
        to_coll = target_by_source.get(edge.to_collection, edge.to_collection)
        conceptual_relationships.append(
            {
                "type": rel_type,
                "fromEntity": entity_name_by_collection.get(from_coll, owl_entity_name(from_coll)),
                "toEntity": entity_name_by_collection.get(to_coll, owl_entity_name(to_coll)),
            }
        )
        # NB: relationships must NOT carry ``collectionName`` (CSI schema
        # forbids it) — only ``edgeCollectionName``.
        physical_relationships[rel_type] = {
            "style": _RELATIONSHIP_STYLE,
            "edgeCollectionName": edge.edge_collection,
        }

    source: Dict[str, Any] = {
        "kind": source_type or "relational",
        "ref": source_ref or config.source_schema,
        "fingerprint": source_fingerprint,
    }
    provenance: Dict[str, Any] = {
        "producer": PRODUCER,
        "producerVersion": producer_version,
        "direction": "forward",
        "source": source,
        "generatedAt": generated_at,
    }
    if confidence is not None:
        provenance["confidence"] = confidence

    return {
        "csiVersion": CSI_VERSION,
        "conceptualModel": {
            "entities": conceptual_entities,
            "relationships": conceptual_relationships,
        },
        "arangoPhysicalMapping": {
            "entities": physical_entities,
            "relationships": physical_relationships,
        },
        "provenance": provenance,
    }


def csi_schema() -> Dict[str, Any]:
    """Load the vendored ``CSI v1`` JSON Schema."""
    text = (
        resources.files("r2g.schemas")
        .joinpath("csi_v1.schema.json")
        .read_text(encoding="utf-8")
    )
    return json.loads(text)


def validate_csi(document: Dict[str, Any]) -> None:
    """Validate ``document`` against the vendored ``CSI v1`` schema.

    Raises:
        jsonschema.ValidationError: if the document is not CSI-valid.
    """
    import jsonschema  # lazy: keep the pure emitter import-light

    jsonschema.validate(instance=document, schema=csi_schema())
