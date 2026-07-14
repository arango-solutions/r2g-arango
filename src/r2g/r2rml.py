"""R2RML mapping emitter (relational -> RDF, for Ontop SPARQL->SQL pushdown).

r2g owns the R2RML capability because R2RML is fundamentally a *relational-source*
artifact: it names the actual SQL tables, columns and primary keys, and r2g is
the only component that knows the source relational schema. A Virtual Knowledge
Graph engine (Ontop) reads this R2RML to answer SPARQL over the live relational
database by rewriting to SQL — no data movement.

Concept/property IRIs default to the same ``urn:arango-sparql:concept#`` namespace
the AQL leg (``arango-sparql-py``) synthesizes from a MappingBundle, so a federated
SPARQL query means the *same thing* on both the relational (Ontop) and graph
(ArangoDB) legs — the two legs share one vocabulary by construction.

See contextual-data-fabric ADR-0001 / M5 implementation-plan WP-A4 (Option 2:
r2g-internal serializer off ``MappingConfig`` + ``Schema``), and the sibling
forward-CSI emitter in :mod:`r2g.csi`.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from .types import MappingConfig, Schema, Table

# Class/property IRI namespace. Defaults to the AQL leg's synthetic concept
# namespace so both federation legs use identical IRIs (see module docstring).
DEFAULT_CONCEPT_BASE = "urn:arango-sparql:concept#"
# Instance/subject IRI namespace (one resource IRI per source row).
DEFAULT_RESOURCE_BASE = "http://r2g.example/resource/"

_PREFIXES = (
    "@prefix rr: <http://www.w3.org/ns/r2rml#> .\n"
    "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
    "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
)


class R2RMLError(ValueError):
    """Raised when a mapping cannot be serialized to valid R2RML."""


def _ttl_string(value: str) -> str:
    """Quote+escape a Turtle string literal."""
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def _xsd_datatype(sql_type: str) -> Optional[str]:
    """Map a SQL type name to an XSD datatype IRI, or None for string-like
    types (R2RML's natural default is fine there)."""
    t = sql_type.strip().lower()
    if t in {"int", "int2", "int4", "int8"} or any(
        k in t for k in ("bigint", "smallint", "integer", "serial")
    ):
        return "xsd:integer"
    if any(k in t for k in ("numeric", "decimal", "money")):
        return "xsd:decimal"
    if any(k in t for k in ("double", "real", "float")):
        return "xsd:double"
    if "bool" in t:
        return "xsd:boolean"
    if "timestamp" in t or "datetime" in t:
        return "xsd:dateTime"
    if t.startswith("date"):
        return "xsd:date"
    if t.startswith("time"):
        return "xsd:time"
    return None


def _mapped_columns(cm, table: Table) -> List[Tuple[str, str, str]]:
    """(source_column, target_property, sql_type) for each emitted column."""
    include = set(cm.include_fields) if cm.include_fields is not None else None
    exclude = set(cm.exclude_fields)
    out: List[Tuple[str, str, str]] = []
    for col in table.columns:
        if include is not None and col.name not in include:
            continue
        if col.name in exclude:
            continue
        prop = cm.field_mappings.get(col.name, col.name)
        out.append((col.name, prop, col.data_type))
    return out


def mapping_to_r2rml(
    config: MappingConfig,
    schema: Schema,
    *,
    concept_base: str = DEFAULT_CONCEPT_BASE,
    resource_base: str = DEFAULT_RESOURCE_BASE,
    source_type: str = "relational",
) -> str:
    """Serialize an r2g mapping to an R2RML Turtle document.

    Args:
        config: the r2g mapping (source tables -> collections, FK edges).
        schema: the source :class:`Schema` (supplies columns, types, PKs —
            required, since R2RML names them).
        concept_base: IRI namespace for classes/properties (default aligns with
            the AQL leg for federation).
        resource_base: IRI namespace for row-level subject IRIs.
        source_type: recorded in the document header for provenance.

    Returns:
        An R2RML Turtle document as a string.

    Raises:
        R2RMLError: when a document entity's source table is absent from the
            schema (R2RML cannot be emitted without it).
    """
    # Conceptual entity name (== target collection) per source table, for
    # resolving FK edge endpoints to the TriplesMaps that host them.
    entity_by_source: Dict[str, str] = {
        cm.source_table: cm.target_collection
        for cm in config.collections.values()
        if cm.collection_type != "edge" and not cm.is_join_table
    }

    # Group FK relationships by the entity that hosts the referencing object
    # map (the "from" side — the table carrying the foreign key).
    rels_by_entity: Dict[str, List[dict]] = {}
    skipped_edges: List[str] = []
    for edge in config.edges:
        from_entity = entity_by_source.get(edge.from_collection)
        to_entity = entity_by_source.get(edge.to_collection)
        if from_entity is None or to_entity is None:
            skipped_edges.append(
                f"{edge.edge_collection} ({edge.from_collection}->{edge.to_collection})"
            )
            continue
        rels_by_entity.setdefault(from_entity, []).append(
            {
                "type": edge.edge_collection,
                "to_entity": to_entity,
                "joins": list(zip(edge.from_fields, edge.to_fields)),
            }
        )

    lines: List[str] = [
        f"# R2RML mapping generated by r2g (source: {source_type}).",
        "# Relational -> RDF for SPARQL->SQL pushdown (e.g. Ontop). No data movement.",
        f"# Concept IRIs: <{concept_base}...>   Resource IRIs: <{resource_base}...>",
        "",
        _PREFIXES,
    ]

    for cm in config.collections.values():
        if cm.collection_type == "edge" or cm.is_join_table:
            continue
        entity = cm.target_collection
        table = schema.tables.get(cm.source_table)
        if table is None:
            raise R2RMLError(
                f"entity {entity!r} maps to source table {cm.source_table!r}, "
                f"which is not present in the schema"
            )

        notes: List[str] = []
        pk = list(table.primary_key or [])
        if not pk:
            # R2RML needs a subject; with no declared PK, template over every
            # column so rows still get distinct (if verbose) IRIs.
            pk = [c.name for c in table.columns]
            notes.append(
                f"# NOTE: {cm.source_table!r} has no primary key; "
                f"subject template uses all columns"
            )

        template = resource_base + entity + "".join(f"/{{{c}}}" for c in pk)

        # Each entry is a complete predicate-object phrase (no trailing
        # separator); the top-level list is joined with " ;" and closed with
        # " .". Turtle permits a trailing ";" before "]" inside bracketed
        # nodes, so inner statements can carry one uniformly.
        preds: List[str] = [
            "    a rr:TriplesMap",
            f"    rr:logicalTable [ rr:tableName {_ttl_string(cm.source_table)} ]",
            (
                "    rr:subjectMap [\n"
                f"        rr:template {_ttl_string(template)} ;\n"
                f"        rr:class <{concept_base}{entity}> ;\n"
                "    ]"
            ),
        ]

        for source_col, prop, sql_type in _mapped_columns(cm, table):
            dtype = _xsd_datatype(sql_type)
            obj = f"rr:column {_ttl_string(source_col)}"
            if dtype is not None:
                obj += f" ; rr:datatype {dtype}"
            preds.append(
                "    rr:predicateObjectMap [\n"
                f"        rr:predicate <{concept_base}{prop}> ;\n"
                f"        rr:objectMap [ {obj} ] ;\n"
                "    ]"
            )

        for rel in rels_by_entity.get(entity, []):
            join_lines = "".join(
                "            rr:joinCondition [ "
                f"rr:child {_ttl_string(child)} ; "
                f"rr:parent {_ttl_string(parent)} ] ;\n"
                for child, parent in rel["joins"]
            )
            preds.append(
                "    rr:predicateObjectMap [\n"
                f"        rr:predicate <{concept_base}{rel['type']}> ;\n"
                "        rr:objectMap [\n"
                f"            rr:parentTriplesMap <#{rel['to_entity']}> ;\n"
                f"{join_lines}"
                "        ] ;\n"
                "    ]"
            )

        if cm.field_expressions:
            computed = ", ".join(
                fe.target for fe in cm.field_expressions if not fe.is_identity
            )
            if computed:
                notes.append(
                    f"# NOTE: computed properties omitted (need SQL): {computed}"
                )

        block = f"<#{entity}>\n" + " ;\n".join(preds) + " ."
        lines.append(block)
        lines.extend(notes)
        lines.append("")

    if skipped_edges:
        lines.append(
            "# Skipped edges (endpoint not a mapped document entity — e.g. "
            "join-table M2M):"
        )
        lines.extend(f"#   {e}" for e in skipped_edges)
        lines.append("")

    return "\n".join(lines) + "\n"
