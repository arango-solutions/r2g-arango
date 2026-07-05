"""Deterministic grounding for the ontology prompt (PRD P11.10).

Runs the Phase 11 denormalization analyzer over a schema and renders its findings
as a compact, advisory digest the prompt builder can hand to the model, so a
proposal is grounded in deterministic evidence (e.g. "zip determines city, state
— consider a Location vertex") rather than name heuristics alone.

Two guardrails carry over from the rest of Phase 10:
- **Structural detectors always run** (repeating groups, 1:1 splits); the
  functional-dependency / multi-valued detectors run only when a ``sampler`` is
  supplied.
- **Classification gate:** columns at/above the redaction threshold are added to
  the analyzer's ``no_sample_columns`` so a Restricted/PII column is never
  value-sampled while computing grounding — mirroring the value-sampling gate.
"""

from __future__ import annotations

from typing import Any, Optional

from r2g.classification import exceeds_threshold, tier_of
from r2g.llm.prompt import DEFAULT_REDACTION_THRESHOLD
from r2g.types import Schema


def _restricted_columns(schema: Schema, threshold: str) -> set[str]:
    """Qualified ``table.column`` names at/above ``threshold`` (never sampled)."""
    out: set[str] = set()
    for tname, table in schema.tables.items():
        for col in table.columns:
            if exceeds_threshold(tier_of(col.classification), threshold):
                out.add(f"{tname}.{col.name}")
    return out


def build_grounding(
    schema: Schema,
    *,
    sampler: Optional[Any] = None,
    redaction_threshold: str = DEFAULT_REDACTION_THRESHOLD,
    min_confidence: float = 0.4,
    sample_limit: int = 10_000,
    max_items: int = 50,
) -> str:
    """Return a deterministic denormalization digest for the prompt (or "").

    ``sampler`` is any :class:`r2g.denorm.DenormSampler` (the value-sampler
    classes). Without one, only structural findings are produced. The result is
    the string emitted by :func:`r2g.denorm.summarize_findings_for_prompt`.
    """
    from r2g.denorm import (
        AnalyzeOptions,
        analyze_denormalization,
        summarize_findings_for_prompt,
    )

    opts = AnalyzeOptions(
        sample=bool(sampler),
        sample_limit=sample_limit,
        min_confidence=min_confidence,
        no_sample_columns=frozenset(_restricted_columns(schema, redaction_threshold)),
    )
    findings = analyze_denormalization(schema, options=opts, sampler=sampler)
    return summarize_findings_for_prompt(findings, max_items=max_items)
