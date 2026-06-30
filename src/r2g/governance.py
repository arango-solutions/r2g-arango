"""Entitlement report, load-time gate, and lineage manifest (PRD Phase 9b).

Lane discipline (see ``docs/internal/PLAN-classification-entitlement.md``): r2g
**advises and emits**; the serving layer **enforces**. This module turns the
9a classification carrier + mosaic recompute into:

- an **entitlement report** over a mapping + annotated schema — every target
  property, its contributing source columns, its mosaic-recomputed sensitivity
  level, and whether it is masked;
- a **threshold gate** that, by default, excludes above-threshold *unmasked*
  fields from a load (overridable with ``allow_sensitive``); and
- a **lineage manifest** — the auditable record of which classified source
  columns crossed the boundary, at what level, and how they were handled.

Everything here is pure (no I/O except the explicit manifest writer) and is
unit-tested against hand-built ``Schema`` / ``MappingConfig`` objects.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from r2g.classification import (
    PUBLIC,
    exceeds_threshold,
    max_sensitivity,
    recompute_mosaic,
    tier_of,
)
from r2g.masking import is_masking_expression, mask_kind_of
from r2g.types import Classification, MappingConfig, Schema

DEFAULT_THRESHOLD = "confidential"


class EntitlementField(BaseModel):
    """One target property's governance posture in a mapping."""

    source_table: str
    target_collection: str
    target_property: str
    # Contributing source columns (one for identity maps, several for fan-in).
    source_columns: list[str] = Field(default_factory=list)
    level: str = PUBLIC
    tags: list[str] = Field(default_factory=list)
    tier: Optional[str] = None
    masked: bool = False
    mask_kind: Optional[str] = None
    # Set by the gate: the field was excluded from the load.
    excluded: bool = False


class EntitlementReport(BaseModel):
    """Pre-load governance report for a project's mapping."""

    project: Optional[str] = None
    threshold: str = DEFAULT_THRESHOLD
    fields: list[EntitlementField] = Field(default_factory=list)
    collection_levels: dict[str, str] = Field(default_factory=dict)
    edge_levels: dict[str, str] = Field(default_factory=dict)

    @property
    def above_threshold(self) -> list[EntitlementField]:
        """Above-threshold fields that are NOT masked (the ones the gate acts on)."""
        return [
            f
            for f in self.fields
            if not f.masked and exceeds_threshold(f.level, self.threshold)
        ]

    @property
    def masked_fields(self) -> list[EntitlementField]:
        return [f for f in self.fields if f.masked]

    def summary(self) -> dict[str, int]:
        return {
            "total_fields": len(self.fields),
            "above_threshold": len(self.above_threshold),
            "masked": len(self.masked_fields),
        }


def _aggregate(classifications: list[Classification]) -> tuple[list[str], Optional[str]]:
    """Union of tags + first tier across several classifications."""
    tags: list[str] = []
    tier: Optional[str] = None
    for clf in classifications:
        for t in clf.tags:
            if t not in tags:
                tags.append(t)
        if tier is None and clf.tier:
            tier = clf.tier
    return tags, tier


def build_entitlement_report(
    config: MappingConfig,
    schema: Schema,
    *,
    threshold: str = DEFAULT_THRESHOLD,
    project: Optional[str] = None,
    tag_levels: Optional[dict[str, str]] = None,
) -> EntitlementReport:
    """Compute the entitlement report for a mapping over an annotated schema.

    Mirrors the mosaic recompute's notion of a "property": a fan-in
    ``FieldExpression`` aggregates the level + tags of its source columns; plain
    kept columns map 1:1. Masking is detected from the field-expression sentinel
    (a masked field is reported but excluded from the gate's above-threshold set).
    """
    mosaic = recompute_mosaic(config, schema, tag_levels=tag_levels)
    report = EntitlementReport(
        project=project,
        threshold=threshold,
        collection_levels=dict(mosaic.collections),
        edge_levels=dict(mosaic.edges),
    )

    for cm in config.collections.values():
        table = schema.tables.get(cm.source_table)
        if table is None:
            continue
        clf_by_col: dict[str, Classification] = {
            c.name: (c.classification or Classification()) for c in table.columns
        }

        # Mask kind per target property (from masking field-expressions).
        mask_by_target: dict[str, Optional[str]] = {}
        expr_sources: dict[str, list[str]] = {}
        for fx in cm.field_expressions:
            if is_masking_expression(fx):
                mask_by_target[fx.target] = mask_kind_of(fx)
            expr_sources[fx.target] = fx.sources or (
                [fx.target] if fx.target in clf_by_col else []
            )

        # Determine kept source columns (include/exclude lists).
        kept = [c.name for c in table.columns]
        if cm.include_fields is not None:
            inc = set(cm.include_fields)
            kept = [c for c in kept if c in inc]
        if cm.exclude_fields:
            exc = set(cm.exclude_fields)
            kept = [c for c in kept if c not in exc]

        # Expression (incl. fan-in) target properties.
        seen_targets: set[str] = set()
        for fx in cm.field_expressions:
            sources = expr_sources.get(fx.target, [])
            clfs = [clf_by_col.get(s, Classification()) for s in sources]
            level = max_sensitivity(tier_of(c, tag_levels=tag_levels) for c in clfs)
            tags, tier = _aggregate(clfs)
            report.fields.append(
                EntitlementField(
                    source_table=cm.source_table,
                    target_collection=cm.target_collection,
                    target_property=fx.target,
                    source_columns=list(sources),
                    level=level,
                    tags=tags,
                    tier=tier,
                    masked=fx.target in mask_by_target,
                    mask_kind=mask_by_target.get(fx.target),
                )
            )
            seen_targets.add(fx.target)

        # Plain kept columns not already covered by an expression target.
        for col in kept:
            target_prop = cm.field_mappings.get(col, col)
            if target_prop in seen_targets:
                continue
            clf = clf_by_col.get(col, Classification())
            report.fields.append(
                EntitlementField(
                    source_table=cm.source_table,
                    target_collection=cm.target_collection,
                    target_property=target_prop,
                    source_columns=[col],
                    level=tier_of(clf, tag_levels=tag_levels),
                    tags=list(clf.tags),
                    tier=clf.tier,
                    masked=target_prop in mask_by_target,
                    mask_kind=mask_by_target.get(target_prop),
                )
            )

    return report


def apply_sensitivity_gate(
    config: MappingConfig,
    report: EntitlementReport,
    *,
    allow_sensitive: bool = False,
) -> tuple[MappingConfig, list[EntitlementField]]:
    """Return a copy of ``config`` with above-threshold unmasked fields excluded.

    The gate is the migration's refusal to *silently* launder sensitive data: by
    default the contributing source columns of every above-threshold, unmasked
    field are added to that collection's ``exclude_fields`` for the run. Passing
    ``allow_sensitive=True`` is the explicit opt-out (config returned unchanged).

    Returns ``(gated_config, excluded_fields)`` where each excluded field has
    ``excluded=True`` set on the returned copies. ``report`` is not mutated.
    """
    if allow_sensitive:
        return config, []

    gated = config.model_copy(deep=True)
    excluded: list[EntitlementField] = []
    for field in report.above_threshold:
        key = _collection_key(gated, field.source_table)
        cm = gated.collections.get(key) if key is not None else None
        if cm is None:
            continue
        for col in field.source_columns:
            if col not in cm.exclude_fields:
                cm.exclude_fields.append(col)
        marked = field.model_copy(update={"excluded": True})
        excluded.append(marked)
    return gated, excluded


def _collection_key(config: MappingConfig, source_table: str) -> Optional[str]:
    """Find the collections-dict key whose mapping sources ``source_table``."""
    for key, cm in config.collections.items():
        if cm.source_table == source_table:
            return key
    return None


def lineage_manifest(report: EntitlementReport) -> dict:
    """Build the machine-readable lineage manifest for a report.

    The auditable record of what crossed the relational→graph boundary: each
    target property, its source columns, classification, mosaic level, and how
    it was handled (masked / excluded / loaded).
    """
    entries = []
    for f in report.fields:
        if f.masked:
            handling = f"masked:{f.mask_kind}" if f.mask_kind else "masked"
        elif f.excluded:
            handling = "excluded"
        elif exceeds_threshold(f.level, report.threshold):
            handling = "loaded-above-threshold"
        else:
            handling = "loaded"
        entries.append(
            {
                "source": [f"{f.source_table}.{c}" for c in f.source_columns],
                "target": f"{f.target_collection}.{f.target_property}",
                "level": f.level,
                "tags": f.tags,
                "tier": f.tier,
                "handling": handling,
            }
        )
    return {
        "project": report.project,
        "threshold": report.threshold,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "collection_levels": report.collection_levels,
        "edge_levels": report.edge_levels,
        "summary": report.summary(),
        "fields": entries,
    }


def write_lineage_manifest(report: EntitlementReport, out_dir: str | Path) -> Path:
    """Write ``lineage.json`` under ``<out_dir>/governance/`` and return its path."""
    gov_dir = Path(out_dir) / "governance"
    gov_dir.mkdir(parents=True, exist_ok=True)
    path = gov_dir / "lineage.json"
    path.write_text(json.dumps(lineage_manifest(report), indent=2), encoding="utf-8")
    return path


__all__ = [
    "DEFAULT_THRESHOLD",
    "EntitlementField",
    "EntitlementReport",
    "build_entitlement_report",
    "apply_sensitivity_gate",
    "lineage_manifest",
    "write_lineage_manifest",
]
