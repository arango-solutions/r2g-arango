"""Opt-in, classification-filtered value sampling for prompts (PRD Phase 10c).

Collects a few example values per column from a live source to help the model
disambiguate cryptic columns (enum-like discriminators, code tables, …). The
guardrail is non-negotiable: **columns at or above the redaction threshold are
never sampled** — sampling can only ever touch columns the digest already shows
in full, so it cannot leak Restricted/PII values.

The sampler is any object exposing ``sample_values(table, column, limit) -> list``
(the value-sampler classes in :mod:`r2g.fk_inference`). Every probe is
best-effort: failures are swallowed and simply yield no sample for that column.
"""

from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable

from r2g.classification import exceeds_threshold, tier_of
from r2g.llm.prompt import DEFAULT_REDACTION_THRESHOLD, DEFAULT_SAMPLES_PER_COLUMN
from r2g.types import Schema


@runtime_checkable
class ValueSampleSource(Protocol):
    """Minimal probe interface for value sampling."""

    def sample_values(self, table: str, column: str, limit: int = 5) -> list[Any]:
        ...


def collect_samples(
    sampler: ValueSampleSource,
    schema: Schema,
    *,
    redaction_threshold: str = DEFAULT_REDACTION_THRESHOLD,
    per_column: int = DEFAULT_SAMPLES_PER_COLUMN,
    max_columns: int = 200,
) -> dict[str, dict[str, list]]:
    """Return a ``table → column → values`` sample map for non-redacted columns.

    Columns whose classification is at/above ``redaction_threshold`` are skipped
    entirely (never sampled). ``max_columns`` bounds the total number of columns
    probed across the whole schema so a wide schema can't fan out into a huge
    number of queries.
    """
    out: dict[str, dict[str, list]] = {}
    probed = 0
    for tname, table in schema.tables.items():
        for col in table.columns:
            if probed >= max_columns:
                return out
            level = tier_of(col.classification)
            if exceeds_threshold(level, redaction_threshold):
                continue
            probed += 1
            try:
                values = sampler.sample_values(tname, col.name, per_column)
            except Exception:  # noqa: BLE001 - sampling is always best-effort
                values = []
            if values:
                out.setdefault(tname, {})[col.name] = list(values)
    return out


def build_sampler_for_source(
    source: Any, *, pg_schema: str = "public", limit: int = 10_000
) -> Optional[ValueSampleSource]:
    """Best-effort value sampler for a registered source, or ``None``.

    Wraps :func:`r2g.fk_inference.create_value_sampler`, resolving the source's
    type / connection string / params. ``pg_schema`` should be the snapshot's
    namespace so queries target the right schema. Returns ``None`` (rather than
    raising) if the source type is unsupported or the optional driver is missing,
    so callers can degrade to a metadata-only prompt.
    """
    from r2g.fk_inference import create_value_sampler

    try:
        return create_value_sampler(
            getattr(source, "source_type", None),
            getattr(source, "connection_string", ""),
            pg_schema=pg_schema,
            source_params=getattr(source, "source_params", None) or {},
            limit=limit,
        )
    except Exception:  # noqa: BLE001 - missing driver / bad DSN → metadata-only
        return None
