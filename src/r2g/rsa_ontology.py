"""Deterministic ontology derivation via ``relational-schema-analyzer`` (RSA).

RSA is the introspection core that was *extracted from* r2g, plus a conceptual /
OWL layer on top. It analyzes a physical relational schema into a conceptual
model (entities / relationships / properties) with a back-reference mapping to
the source tables, emitting a tool-contract bundle
``{conceptualSchema, physicalMapping, metadata}``.

This module is the r2g-side adapter for that library. It:

1. Feeds an r2g :class:`~r2g.types.Schema` directly to RSA's analyzer. r2g's
   ``Schema`` *subclasses* RSA's ``PhysicalSchema`` (see
   ``docs/internal/DESIGN-rsa-compat-layer.md``), so no conversion is needed.
2. Converts the resulting bundle into an :class:`~r2g.llm.base.OntologyProposal`.

The proposal is then run through the **same** validated
:func:`r2g.llm.ontology.proposal_to_mapping` "hallucination gate" as the LLM
path, so an RSA-derived mapping is guaranteed loadable and never diverges from a
schema-validated ``MappingConfig``.

Unlike the LLM path this is **deterministic and offline by default** — no rows
and no network. An optional ``provider`` enables RSA's own additive LLM
refinement (better semantic names, embed / n-ary hints); refinement never fails
the analysis (RSA falls back to its baseline internally).

Mechanical mapping (bundle → proposal):

- Each conceptual **entity** → a :class:`ProposedCollection` whose
  ``target_collection`` is the entity's semantic (PascalCase) name and whose
  ``source_table`` is the physical table it maps from.
- A property whose conceptual name differs from its physical column →
  a :class:`ProposedRename` (only happens under LLM refinement; the baseline
  keeps column names verbatim).
- A ``FOREIGN_KEY`` relationship → a :class:`ProposedEdge` (deduplicated against
  the baseline's declared-FK edges by the gate).
- A ``JOIN_TABLE`` relationship → the join table is flagged ``is_join_table`` and
  the many-to-many is recorded as an advisory note. The current loader
  materializes a join table as a vertex + two FK edges (native
  join-table-as-edge is available in the transformer but not wired into the
  streaming pipeline), so — like embed hints — the m2m is surfaced for review
  rather than applied automatically.
"""

from __future__ import annotations

from typing import Any

from r2g.llm.base import (
    OntologyProposal,
    ProposedCollection,
    ProposedEdge,
    ProposedRename,
)
from r2g.types import Schema

RSA_IMPORT_HINT = (
    "relational-schema-analyzer is not installed. It is a core r2g dependency, so "
    "reinstalling r2g (`pip install r2g-arango`) should restore it; for local "
    "development use `pip install -e ../relational-schema-analyzer`."
)

_DEFAULT_CONFIDENCE = 0.9


def _require_rsa() -> Any:
    """Import the RSA package, raising a friendly :class:`ImportError` if absent."""
    try:
        import relational_schema_analyzer as rsa  # noqa: PLC0415 - lazy optional dep
    except ImportError as err:  # pragma: no cover - exercised via unit test monkeypatch
        raise ImportError(RSA_IMPORT_HINT) from err
    return rsa


def analyze_schema(
    schema: Schema,
    *,
    provider: str | None = None,
    model: str | None = None,
    api_key: str | None = None,
) -> dict[str, Any]:
    """Run RSA over *schema* and return the tool-contract bundle.

    ``provider`` is ``None`` for the deterministic baseline (no LLM, no network),
    or a provider name RSA understands (``"openai"`` / ``"anthropic"`` /
    ``"openrouter"``) for additive refinement. Refinement errors are absorbed by
    RSA and fall back to the baseline; the returned bundle is always valid.
    """
    rsa = _require_rsa()
    # r2g's Schema now *subclasses* RSA's PhysicalSchema (see
    # docs/internal/DESIGN-rsa-compat-layer.md), so it is passed straight to the
    # analyzer — no JSON round-trip. RSA reads the shared physical fields
    # (tables/columns/foreign_keys/…); r2g's extra Phase-9 `classification` is
    # simply ignored by the analyzer.
    analyzer = rsa.RelationalSchemaAnalyzer(
        llm_provider=provider,
        model=model,
        api_key=api_key,
    )
    return analyzer.analyze(schema).to_bundle()


def _confidence(metadata: dict[str, Any]) -> float:
    try:
        value = float(metadata.get("confidence", _DEFAULT_CONFIDENCE))
    except (TypeError, ValueError):
        return _DEFAULT_CONFIDENCE
    return value if 0.0 <= value <= 1.0 else _DEFAULT_CONFIDENCE


def bundle_to_proposal(bundle: dict[str, Any]) -> OntologyProposal:
    """Convert an RSA tool-contract bundle into an :class:`OntologyProposal`.

    The proposal only *enriches* the Auto-Map baseline; the downstream gate drops
    anything that does not resolve against the real schema, so this converter is
    deliberately permissive and defensive about the bundle's shape.
    """
    mapping = bundle.get("physicalMapping") or {}
    metadata = bundle.get("metadata") or {}
    entity_map = mapping.get("entities") or {}
    rel_map = mapping.get("relationships") or {}
    confidence = _confidence(metadata)

    collections: list[ProposedCollection] = []
    renames: list[ProposedRename] = []
    edges: list[ProposedEdge] = []
    notes: list[str] = []
    join_tables: set[str] = set()

    if isinstance(entity_map, dict):
        for entity_name, em in entity_map.items():
            if not isinstance(em, dict):
                continue
            table = em.get("tableName")
            if not table:
                continue
            collections.append(
                ProposedCollection(
                    source_table=table,
                    target_collection=str(entity_name),
                    collection_type="document",
                    rationale="relational-schema-analyzer: table \u2192 conceptual entity",
                    confidence=confidence,
                )
            )
            props = em.get("properties") or {}
            if isinstance(props, dict):
                for concept_name, pd in props.items():
                    if not isinstance(pd, dict):
                        continue
                    field = pd.get("field") or pd.get("columnName")
                    if field and concept_name and str(concept_name) != str(field):
                        renames.append(
                            ProposedRename(
                                source_table=table,
                                column=str(field),
                                target_property=str(concept_name),
                                rationale="relational-schema-analyzer: property naming",
                                confidence=confidence,
                            )
                        )

    if isinstance(rel_map, dict):
        for rel_type, rm in rel_map.items():
            if not isinstance(rm, dict):
                continue
            style = rm.get("style")
            if style == "FOREIGN_KEY":
                frm = rm.get("fromTable")
                to = rm.get("toTable")
                ff = rm.get("fromColumns") or []
                tf = rm.get("toColumns") or []
                if frm and to and ff and tf:
                    edges.append(
                        ProposedEdge(
                            edge_collection=str(rel_type),
                            from_collection=str(frm),
                            to_collection=str(to),
                            from_fields=[str(c) for c in ff],
                            to_fields=[str(c) for c in tf],
                            rationale="relational-schema-analyzer: foreign key",
                            confidence=confidence,
                        )
                    )
            elif style == "JOIN_TABLE":
                jt = rm.get("joinTable")
                if jt:
                    join_tables.add(str(jt))
                attrs = rm.get("attributeColumns") or []
                note = f"Many-to-many '{rel_type}' via join table '{jt}'"
                if attrs:
                    note += f" (attribute columns: {', '.join(str(a) for a in attrs)})"
                note += " \u2014 loaded as a vertex + FK edges; review for edge-collection modeling."
                notes.append(note)

    # Flag detected join tables. Ones that are also entities get the flag set;
    # ones that are not entities are added explicitly so the flag survives the
    # gate (which only touches collections present in the proposal).
    known = {pc.source_table for pc in collections}
    for pc in collections:
        if pc.source_table in join_tables:
            pc.is_join_table = True
    for jt in sorted(join_tables - known):
        collections.append(
            ProposedCollection(
                source_table=jt,
                target_collection=jt,
                collection_type="document",
                is_join_table=True,
                rationale="relational-schema-analyzer: join table (many-to-many)",
                confidence=confidence,
            )
        )

    patterns = metadata.get("detectedPatterns")
    if isinstance(patterns, list) and patterns:
        notes.append("Detected patterns: " + ", ".join(str(p) for p in patterns))
    assumptions = metadata.get("assumptions")
    if isinstance(assumptions, list):
        for a in assumptions:
            notes.append(f"Assumption: {a}")
    if metadata.get("reviewRequired"):
        notes.append("relational-schema-analyzer flagged this schema for manual review.")

    return OntologyProposal(
        collections=collections,
        edges=edges,
        renames=renames,
        notes=notes,
    )


def propose_ontology_from_schema(
    schema: Schema,
    *,
    provider: str | None = None,
    model: str | None = None,
    api_key: str | None = None,
) -> tuple[OntologyProposal, dict[str, Any]]:
    """Analyze *schema* with RSA and return ``(proposal, bundle_metadata)``.

    The metadata carries RSA's provenance (confidence, detected patterns,
    fingerprint, review flags, and any LLM-refinement info) for the caller to
    record alongside the proposed mapping.
    """
    bundle = analyze_schema(schema, provider=provider, model=model, api_key=api_key)
    proposal = bundle_to_proposal(bundle)
    metadata = bundle.get("metadata") or {}
    return proposal, metadata
