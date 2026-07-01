"""Convert an :class:`OntologyProposal` into a *validated* ``MappingConfig``.

This is the hallucination gate. The strategy guarantees the result is always a
valid, loadable mapping (worst case: equivalent to Auto-Map):

1. Start from :meth:`ConfigManager.generate_default_config` — a complete, valid
   baseline covering every (non-partition) table. The proposal only *enriches*
   it; nothing the model omits is lost.
2. Apply each proposed item behind an explicit guard mirroring
   :func:`validate_config` (table/column existence, reserved attributes, unique
   edge names). Anything that references something not in the real schema is
   dropped and recorded in ``notes`` — never loaded.
3. Run :func:`validate_config` on the result. If (despite the guards) issues
   remain, drop the proposed edges and renames and fall back to the validated
   baseline, recording why.

The model proposes; this function disposes.
"""

from __future__ import annotations

from r2g.config import ConfigManager, validate_config
from r2g.llm.base import OntologyProposal
from r2g.types import (
    RESERVED_ATTRIBUTES,
    EdgeDefinition,
    MappingConfig,
    Schema,
)


def proposal_to_mapping(
    proposal: OntologyProposal,
    schema: Schema,
    *,
    source_schema: str = "public",
) -> tuple[MappingConfig, list[str]]:
    """Build a validated ``MappingConfig`` from a proposal.

    Returns the config plus human-readable notes describing every applied,
    skipped, or dropped suggestion. The returned config always passes
    :func:`validate_config`.
    """
    notes: list[str] = list(proposal.notes)
    config = ConfigManager.generate_default_config(schema, source_schema=source_schema)

    cols_by_table: dict[str, set[str]] = {
        name: {c.name for c in table.columns} for name, table in schema.tables.items()
    }

    _apply_collections(proposal, config, schema, notes)
    _apply_renames(proposal, config, cols_by_table, notes)
    _apply_edges(proposal, config, schema, cols_by_table, notes)
    _record_embeds(proposal, schema, notes)

    issues = validate_config(schema, config)
    if issues:
        # Guards should prevent this; if a proposed enrichment still broke
        # validation, fall back to the proven-valid baseline (Auto-Map) rather
        # than ship an invalid mapping.
        notes.append(
            "Proposal produced validation issues; reverted to Auto-Map baseline. "
            + "; ".join(issues[:5])
        )
        config = ConfigManager.generate_default_config(schema, source_schema=source_schema)
    return config, notes


def _apply_collections(
    proposal: OntologyProposal,
    config: MappingConfig,
    schema: Schema,
    notes: list[str],
) -> None:
    for pc in proposal.collections:
        if pc.source_table not in schema.tables:
            notes.append(
                f"Dropped collection for unknown table '{pc.source_table}' (hallucinated)."
            )
            continue
        cm = config.collections.get(pc.source_table)
        if cm is None:
            # Partition child or otherwise collapsed table; skip silently-ish.
            notes.append(
                f"Skipped collection '{pc.source_table}': not an independent collection "
                f"in the baseline (e.g. a partition child)."
            )
            continue
        ctype = pc.collection_type if pc.collection_type in ("document", "edge") else "document"
        if ctype != cm.collection_type:
            notes.append(
                f"Set '{pc.source_table}' collection_type -> {ctype}"
                + (f" ({pc.rationale})" if pc.rationale else "")
            )
            cm.collection_type = ctype
        if pc.target_collection and pc.target_collection != cm.target_collection:
            notes.append(
                f"Renamed collection '{pc.source_table}' -> '{pc.target_collection}'"
                + (f" ({pc.rationale})" if pc.rationale else "")
            )
            cm.target_collection = pc.target_collection
        if pc.is_join_table and not cm.is_join_table:
            cm.is_join_table = True
            notes.append(f"Marked '{pc.source_table}' as a join table.")


def _apply_renames(
    proposal: OntologyProposal,
    config: MappingConfig,
    cols_by_table: dict[str, set[str]],
    notes: list[str],
) -> None:
    for rn in proposal.renames:
        cm = config.collections.get(rn.source_table)
        if cm is None or rn.source_table not in cols_by_table:
            notes.append(
                f"Dropped rename on unknown table '{rn.source_table}' (hallucinated)."
            )
            continue
        if rn.column not in cols_by_table[rn.source_table]:
            notes.append(
                f"Dropped rename of unknown column "
                f"'{rn.source_table}.{rn.column}' (hallucinated)."
            )
            continue
        if rn.target_property in RESERVED_ATTRIBUTES:
            notes.append(
                f"Dropped rename '{rn.source_table}.{rn.column}' -> "
                f"'{rn.target_property}': reserved ArangoDB attribute."
            )
            continue
        if not rn.target_property or rn.target_property == rn.column:
            continue
        cm.field_mappings[rn.column] = rn.target_property
        notes.append(
            f"Rename '{rn.source_table}.{rn.column}' -> '{rn.target_property}'"
            + (f" ({rn.rationale})" if rn.rationale else "")
        )


def _apply_edges(
    proposal: OntologyProposal,
    config: MappingConfig,
    schema: Schema,
    cols_by_table: dict[str, set[str]],
    notes: list[str],
) -> None:
    existing_names = {e.edge_collection for e in config.edges}
    # Dedupe by the (from, to, fields) signature so a proposed edge that merely
    # restates a declared FK does not create a duplicate edge collection.
    existing_sigs = {_edge_signature(e) for e in config.edges}

    for pe in proposal.edges:
        if pe.from_collection not in schema.tables:
            notes.append(
                f"Dropped edge '{pe.edge_collection}': unknown from-table "
                f"'{pe.from_collection}' (hallucinated)."
            )
            continue
        if pe.to_collection not in schema.tables:
            notes.append(
                f"Dropped edge '{pe.edge_collection}': unknown to-table "
                f"'{pe.to_collection}' (hallucinated)."
            )
            continue
        if not pe.from_fields or not pe.to_fields:
            notes.append(f"Dropped edge '{pe.edge_collection}': missing join fields.")
            continue
        bad_from = [f for f in pe.from_fields if f not in cols_by_table[pe.from_collection]]
        bad_to = [f for f in pe.to_fields if f not in cols_by_table[pe.to_collection]]
        if bad_from or bad_to:
            notes.append(
                f"Dropped edge '{pe.edge_collection}': unknown join column(s) "
                f"{bad_from + bad_to} (hallucinated)."
            )
            continue

        name = pe.edge_collection or f"{pe.from_collection}_to_{pe.to_collection}"
        candidate = EdgeDefinition(
            edge_collection=name,
            from_collection=pe.from_collection,
            to_collection=pe.to_collection,
            from_fields=list(pe.from_fields),
            to_fields=list(pe.to_fields),
        )
        if _edge_signature(candidate) in existing_sigs:
            continue  # already covered by a declared FK edge
        # Disambiguate a clashing name.
        if name in existing_names:
            name = f"{name}_{'_'.join(pe.from_fields)}"
            candidate.edge_collection = name
            if name in existing_names:
                notes.append(f"Dropped edge '{pe.edge_collection}': duplicate edge name.")
                continue
        config.edges.append(candidate)
        existing_names.add(name)
        existing_sigs.add(_edge_signature(candidate))
        notes.append(
            f"Added relationship '{name}': {pe.from_collection} -> {pe.to_collection}"
            + (f" ({pe.rationale})" if pe.rationale else "")
        )


def _record_embeds(proposal: OntologyProposal, schema: Schema, notes: list[str]) -> None:
    for em in proposal.embeds:
        if em.parent_table not in schema.tables or em.child_table not in schema.tables:
            continue
        as_prop = f" as '{em.as_property}'" if em.as_property else ""
        notes.append(
            f"Embed hint (advisory): embed '{em.child_table}' into "
            f"'{em.parent_table}'{as_prop}"
            + (f" ({em.rationale})" if em.rationale else "")
        )


def _edge_signature(edge: EdgeDefinition) -> tuple[str, str, tuple[str, ...], tuple[str, ...]]:
    return (
        edge.from_collection,
        edge.to_collection,
        tuple(edge.from_fields),
        tuple(edge.to_fields),
    )
