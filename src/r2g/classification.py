"""Sensitivity lattice + mosaic recomputation (PRD Phase 9a).

r2g is a migration tool, not a runtime authorization engine. This module is the
deterministic *data-model thread* that lets r2g carry governance metadata across
the relational→graph boundary:

- a small ordered **sensitivity lattice** (``public < internal < confidential <
  restricted``),
- a configurable **tag/tier FQN → level** map (defaults for common OpenMetadata
  tags, overridable per project),
- ``max_sensitivity`` / ``exceeds_threshold`` helpers, and
- **mosaic recomputation**: the sensitivity of a graph entity assembled from
  several source columns (a vertex, an edge, a fan-in property) is the *max* of
  its contributors — never blindly inherited — because denormalization can
  reveal a combined picture no single column did.

It operates purely on already-annotated :class:`~r2g.types.Schema` columns and a
:class:`~r2g.types.MappingConfig`; it performs no I/O and never enforces access.
"""

from __future__ import annotations

from typing import Iterable, Optional

from pydantic import BaseModel, Field

from r2g.types import Classification, CollectionMapping, MappingConfig, Schema

# Ordered low→high. Index is the rank; comparisons use the rank.
SENSITIVITY_ORDER: tuple[str, ...] = ("public", "internal", "confidential", "restricted")

_RANK: dict[str, int] = {level: i for i, level in enumerate(SENSITIVITY_ORDER)}

# Lowest level (no signal). Used when nothing maps.
PUBLIC: str = SENSITIVITY_ORDER[0]

# Default tag/tier FQN prefix → lattice level. Matched case-insensitively as a
# dotted-prefix (so ``PII.Sensitive`` matches ``pii``). Org-specific; overridable
# per call. Longest matching prefix wins.
DEFAULT_TAG_LEVELS: dict[str, str] = {
    "pii": "restricted",
    "phi": "restricted",
    "personaldata.sensitivepersonal": "restricted",
    "personaldata.special": "restricted",
    "personaldata": "confidential",
    "sensitive": "confidential",
    "confidential": "confidential",
    "restricted": "restricted",
    "tier.tier1": "confidential",
    "tier.tier2": "internal",
    "tier.tier3": "internal",
    "tier.tier4": "public",
    "tier.tier5": "public",
    "internal": "internal",
    "public": "public",
}


def sensitivity_rank(level: str) -> int:
    """Rank of a lattice level (unknown levels rank as ``public``)."""
    return _RANK.get(level.lower(), 0)


def normalize_level(level: str) -> str:
    """Canonicalize a level string to a known lattice level (else ``public``)."""
    low = level.lower()
    return low if low in _RANK else PUBLIC


def max_sensitivity(levels: Iterable[str]) -> str:
    """Return the highest level among ``levels`` (the mosaic rollup rule)."""
    best = PUBLIC
    best_rank = 0
    for lvl in levels:
        r = sensitivity_rank(lvl)
        if r > best_rank:
            best, best_rank = normalize_level(lvl), r
    return best


def exceeds_threshold(level: str, threshold: str) -> bool:
    """True when ``level`` is at or above ``threshold`` on the lattice."""
    return sensitivity_rank(level) >= sensitivity_rank(threshold)


def _level_for_fqn(fqn: str, tag_levels: dict[str, str]) -> Optional[str]:
    """Map a single tag/tier FQN to a level via longest matching dotted-prefix."""
    low = fqn.strip().lower()
    if not low:
        return None
    best: Optional[str] = None
    best_len = -1
    for prefix, level in tag_levels.items():
        # Match exact or as a dotted prefix ("pii" matches "pii.sensitive").
        if (low == prefix or low.startswith(prefix + ".")) and len(prefix) > best_len:
            best, best_len = level, len(prefix)
    return best


def annotate_schema(
    schema: Schema,
    classifications: dict[str, dict[str, Classification]],
) -> int:
    """Stamp ``Column.classification`` onto a schema from a resolved map.

    Mutates ``schema`` in place, merging ``classifications`` (table → column →
    :class:`Classification`) onto matching columns. Tables/columns absent from
    the map are left untouched. Returns the number of columns annotated so the
    caller can report coverage. Matching is exact on table and column names.
    """
    annotated = 0
    for table_name, col_map in classifications.items():
        table = schema.tables.get(table_name)
        if table is None:
            continue
        for column in table.columns:
            clf = col_map.get(column.name)
            if clf is not None and not clf.is_empty:
                column.classification = clf
                annotated += 1
    return annotated


def tier_of(
    classification: Optional[Classification],
    *,
    tag_levels: Optional[dict[str, str]] = None,
) -> str:
    """Map a column/asset classification to a lattice level (``public`` if none).

    Considers every tag FQN and the tier FQN; returns the max matched level.
    Unmapped tags contribute nothing (they do not silently escalate), which keeps
    the result predictable and overridable.
    """
    if classification is None:
        return PUBLIC
    levels_map = tag_levels or DEFAULT_TAG_LEVELS
    candidates: list[str] = []
    for fqn in classification.tags:
        lvl = _level_for_fqn(fqn, levels_map)
        if lvl:
            candidates.append(lvl)
    if classification.tier:
        lvl = _level_for_fqn(classification.tier, levels_map)
        if lvl:
            candidates.append(lvl)
    return max_sensitivity(candidates) if candidates else PUBLIC


# ── Mosaic recomputation over a mapping ──────────────────────────────


class MosaicLevels(BaseModel):
    """Recomputed entity sensitivity levels for a mapping.

    ``collections`` and ``edges`` are keyed by source-table / edge-collection
    name; ``fields`` is keyed ``"collection.property"``. Every value is a lattice
    level (max of contributing source columns).
    """

    collections: dict[str, str] = Field(default_factory=dict)
    edges: dict[str, str] = Field(default_factory=dict)
    fields: dict[str, str] = Field(default_factory=dict)

    def above(self, threshold: str) -> dict[str, str]:
        """Return the ``fields`` entries at or above ``threshold``."""
        return {k: v for k, v in self.fields.items() if exceeds_threshold(v, threshold)}


def _column_levels(schema: Schema, tag_levels: dict[str, str]) -> dict[str, dict[str, str]]:
    """Per-table ``{column: level}`` from annotated schema columns."""
    out: dict[str, dict[str, str]] = {}
    for tname, table in schema.tables.items():
        out[tname] = {c.name: tier_of(c.classification, tag_levels=tag_levels) for c in table.columns}
    return out


def _kept_columns(table_cols: dict[str, str], cm: CollectionMapping) -> list[str]:
    """Columns that survive a collection mapping's include/exclude lists."""
    names = list(table_cols.keys())
    if cm.include_fields is not None:
        names = [n for n in names if n in set(cm.include_fields)]
    if cm.exclude_fields:
        excl = set(cm.exclude_fields)
        names = [n for n in names if n not in excl]
    return names


def recompute_mosaic(
    config: MappingConfig,
    schema: Schema,
    *,
    tag_levels: Optional[dict[str, str]] = None,
) -> MosaicLevels:
    """Recompute entity sensitivity over a mapping (the mosaic = max rule).

    - A **property** level is the max over its source columns: a fan-in
      ``FieldExpression`` (several sources → one property) takes the max of those
      sources; a plain kept column takes its own level.
    - A **collection** level is the max over the property levels it carries.
    - An **edge** level is the max over the from/to endpoint columns it joins on
      plus the endpoint collection levels.
    """
    levels_map = tag_levels or DEFAULT_TAG_LEVELS
    col_levels = _column_levels(schema, levels_map)
    result = MosaicLevels()

    for cm in config.collections.values():
        table_cols = col_levels.get(cm.source_table, {})
        if not table_cols:
            continue
        prop_levels: list[str] = []

        # Fan-in / expression properties: max of declared sources.
        expr_targets: set[str] = set()
        for fx in cm.field_expressions:
            sources = fx.sources or ([fx.target] if fx.target in table_cols else [])
            lvl = max_sensitivity(table_cols.get(s, PUBLIC) for s in sources)
            result.fields[f"{cm.source_table}.{fx.target}"] = lvl
            prop_levels.append(lvl)
            expr_targets.add(fx.target)

        # Plain kept columns (not already covered by an expression target).
        for col in _kept_columns(table_cols, cm):
            if col in expr_targets:
                continue
            lvl = table_cols.get(col, PUBLIC)
            result.fields[f"{cm.source_table}.{col}"] = lvl
            prop_levels.append(lvl)

        result.collections[cm.source_table] = max_sensitivity(prop_levels)

    for edge in config.edges:
        contributors: list[str] = [
            result.collections.get(edge.from_collection, PUBLIC),
            result.collections.get(edge.to_collection, PUBLIC),
        ]
        from_cols = col_levels.get(edge.from_collection, {})
        to_cols = col_levels.get(edge.to_collection, {})
        contributors.extend(from_cols.get(f, PUBLIC) for f in edge.from_fields)
        contributors.extend(to_cols.get(f, PUBLIC) for f in edge.to_fields)
        label = edge.edge_collection or f"{edge.from_collection}_to_{edge.to_collection}"
        result.edges[label] = max_sensitivity(contributors)

    return result
