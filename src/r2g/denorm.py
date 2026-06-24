"""Deterministic denormalization & normal-form analysis (PRD Phase 11).

``ConfigManager.generate_default_config`` maps every relational table 1:1 to a
document collection. That is the right *default*, but it is blind to tables that
carry an *embedded lookup* (a non-key column functionally determines other
non-key columns, e.g. ``zip → city, state``) or a *repeating group* (a numbered
column family like ``phone1/phone2/phone3``). Loaded as-is, those become
redundant properties instead of a cleaner graph (a shared vertex + an edge, or
an array / child collection).

This module is the **deterministic** (no-LLM) analyzer that detects those
smells and emits scored, evidence-backed :class:`DenormFinding` objects with a
recommended graph remedy. It *advises*; it never rewrites the schema or data.

Design mirrors :mod:`r2g.fk_inference`:

- :func:`analyze_denormalization` takes a :class:`r2g.types.Schema` and an
  optional ``sampler``. Structural detectors (repeating groups) run with no
  sampler. The functional-dependency detector (the flagship "embedded lookup"
  case) needs bounded data probes and is therefore sampler-gated.
- The ``sampler`` is the same object :func:`r2g.fk_inference.create_value_sampler`
  builds (Postgres / MySQL / SQL Server / CSV); it now also exposes the two
  bounded probes this analyzer needs (:class:`DenormSampler`).
- Findings are ranked by confidence and filtered by ``min_confidence``.

This is also the deterministic *grounding* for the Phase 10 LLM proposal; it has
no LLM dependency of its own.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from r2g.config import pg_type_to_json_type
from r2g.log import get_logger
from r2g.types import Schema, Table

logger = get_logger(__name__)


# ── Public model ─────────────────────────────────────────────────────


class DenormFinding(BaseModel):
    """A single denormalization smell produced by :func:`analyze_denormalization`.

    ``columns`` lists every column the finding concerns. For an
    ``embedded_lookup`` finding, ``determinant`` and ``dependents`` split those
    columns into "the column that determines" and "the columns it determines",
    which the (later) remediation scaffolding uses to extract a shared vertex.
    """

    kind: str  # repeating_group | embedded_lookup
    table: str
    columns: list[str]
    recommended_action: str  # extract_vertex | embed_array | split_column | merge
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[str] = Field(default_factory=list)
    determinant: list[str] = Field(default_factory=list)
    dependents: list[str] = Field(default_factory=list)


# ── Options ─────────────────────────────────────────────────────────


@dataclass
class AnalyzeOptions:
    """Knobs that shape what the analyzer considers / returns.

    ``no_sample_columns`` and ``is_sampleable`` are the classification gate: a
    column that must not be value-sampled (e.g. a Phase-9 Restricted / PII
    column) is never passed to the sampler. ``no_sample_columns`` accepts bare
    column names and ``"table.column"`` qualified names; ``is_sampleable`` is the
    forward hook Phase 9 plugs into.
    """

    sample: bool = False
    sample_limit: int = 10_000
    min_confidence: float = 0.4
    fd_threshold: float = 0.98
    determinant_max_distinct_ratio: float = 0.5
    max_determinants_per_table: int = 12
    no_sample_columns: frozenset[str] = frozenset()
    is_sampleable: Optional[Callable[[str, str], bool]] = field(default=None)


# ── Sampler protocol ────────────────────────────────────────────────


@runtime_checkable
class DenormSampler(Protocol):
    """Bounded data probes the sampling detectors need.

    Implemented by the same value-sampler classes used for FK inference
    (:class:`r2g.fk_inference.PostgresValueSampler` and siblings). Each probe is
    bounded (a small ``LIMIT``/``TOP`` sample) and resilient — it returns
    ``None`` on any failure so the analyzer degrades to structural signals
    rather than crashing.
    """

    def distinct_ratio(self, table: str, column: str) -> Optional[float]:
        """Fraction of distinct non-null values in ``column`` (``ndistinct/n``)."""
        ...

    def group_single_valued(
        self, table: str, determinant_columns: list[str], dependent_column: str
    ) -> Optional[float]:
        """Fraction of ``determinant_columns`` groups with exactly one
        ``dependent_column`` value — i.e. how strongly the determinant
        functionally determines the dependent (1.0 == perfect FD)."""
        ...


# ── Entry point ─────────────────────────────────────────────────────


def analyze_denormalization(
    schema: Schema,
    *,
    options: Optional[AnalyzeOptions] = None,
    sampler: Optional[DenormSampler] = None,
) -> list[DenormFinding]:
    """Return ranked denormalization findings for ``schema``.

    Structural detectors (repeating groups) always run. The functional-dependency
    detector (embedded lookups) runs only when ``options.sample`` is set and a
    ``sampler`` is provided. Results are deduplicated per
    ``(kind, table, tuple(columns))``, sorted by confidence descending, and
    filtered by ``options.min_confidence``.
    """
    opts = options or AnalyzeOptions()
    sampleable = _make_sampleable(opts)

    findings: list[DenormFinding] = []
    for table in schema.tables.values():
        findings.extend(_detect_repeating_groups(table, opts))

    if opts.sample and sampler is not None:
        for table in schema.tables.values():
            findings.extend(_detect_embedded_lookups(table, sampler, opts, sampleable))

    deduped = _dedupe(findings)
    ranked = sorted(deduped, key=lambda f: f.confidence, reverse=True)
    return [f for f in ranked if f.confidence >= opts.min_confidence]


# ── Detector: repeating groups (structural, no sampling) ─────────────


_SUFFIX_RE = re.compile(r"^(?P<stem>.*?)[ _-]?(?P<num>\d+)$")


def _detect_repeating_groups(table: Table, opts: AnalyzeOptions) -> list[DenormFinding]:
    """Detect numbered column families (``phone1/phone2``, ``addr_line_1..3``).

    A family is two or more columns that share a stem and differ only by a
    trailing integer, with mutually compatible JSON types. Single columns whose
    name merely *ends* in a digit (``md5``, ``sha256``) never group, so they do
    not misfire.
    """
    families: dict[str, list[tuple[int, str]]] = {}
    for col in table.columns:
        m = _SUFFIX_RE.match(col.name.lower())
        if not m:
            continue
        stem = m.group("stem")
        if len(stem) < 2:
            continue
        # Preserve the original (non-lowered) column name for output.
        families.setdefault(stem, []).append((int(m.group("num")), col.name))

    out: list[DenormFinding] = []
    for stem, members in families.items():
        if len(members) < 2:
            continue
        # Distinct suffixes only (guard against a name colliding with itself).
        if len({n for n, _ in members}) < 2:
            continue
        members.sort(key=lambda t: t[0])
        member_cols = [name for _, name in members]

        json_types = {
            pg_type_to_json_type(c.data_type)
            for c in table.columns
            if c.name in set(member_cols)
        }
        same_type = len(json_types) == 1

        confidence = 0.6 + 0.1 * (len(member_cols) - 2)
        if same_type:
            confidence += 0.1
        confidence = round(min(confidence, 0.95), 3)
        if confidence < opts.min_confidence:
            continue

        evidence = [
            f"columns {', '.join(member_cols)} form a numbered family on stem '{stem}'",
        ]
        if same_type:
            evidence.append(f"all share JSON type '{next(iter(json_types))}'")
        out.append(
            DenormFinding(
                kind="repeating_group",
                table=table.name,
                columns=member_cols,
                recommended_action="embed_array",
                confidence=confidence,
                evidence=evidence,
            )
        )
    return out


# ── Detector: embedded lookups / functional dependencies (sampled) ───


def _detect_embedded_lookups(
    table: Table,
    sampler: DenormSampler,
    opts: AnalyzeOptions,
    sampleable: Callable[[str, str], bool],
) -> list[DenormFinding]:
    """Detect a non-key column that functionally determines other non-key
    columns (2NF/3NF violation = an embedded lookup that wants its own vertex).

    Candidate determinants are non-PK columns that *repeat* (low distinct ratio);
    a dependent is any other non-PK column that is single-valued within the
    determinant's groups. Unique columns (ids, emails) naturally fail both tests.
    """
    pk_set = set(table.primary_key)
    non_pk = [c for c in table.columns if c.name not in pk_set]

    ratios: dict[str, float] = {}
    for col in non_pk:
        if not sampleable(table.name, col.name):
            continue
        r = _safe_probe(sampler.distinct_ratio, table.name, col.name)
        if r is not None:
            ratios[col.name] = r

    determinants = sorted(
        (
            name
            for name, r in ratios.items()
            if 0.0 < r <= opts.determinant_max_distinct_ratio
        ),
        key=lambda n: ratios[n],
    )[: opts.max_determinants_per_table]

    out: list[DenormFinding] = []
    for det in determinants:
        dependents: list[str] = []
        fd_scores: list[float] = []
        for col in non_pk:
            if col.name == det or col.name not in ratios:
                continue
            score = _safe_probe(
                sampler.group_single_valued, table.name, [det], col.name
            )
            if score is None:
                continue
            if score >= opts.fd_threshold:
                dependents.append(col.name)
                fd_scores.append(score)

        if not dependents:
            continue

        avg_fd = sum(fd_scores) / len(fd_scores)
        confidence = 0.5 + 0.1 * min(len(dependents), 3)
        if ratios[det] <= 0.2:
            confidence += 0.1
        if avg_fd >= 0.999:
            confidence += 0.1
        confidence = round(min(confidence, 0.95), 3)
        if confidence < opts.min_confidence:
            continue

        evidence = [
            f"'{det}' repeats (distinct ratio {ratios[det]:.2f})",
            *(
                f"'{det}' → '{dep}' single-valued in {score:.0%} of groups"
                for dep, score in zip(dependents, fd_scores)
            ),
        ]
        out.append(
            DenormFinding(
                kind="embedded_lookup",
                table=table.name,
                columns=[det, *dependents],
                recommended_action="extract_vertex",
                confidence=confidence,
                evidence=evidence,
                determinant=[det],
                dependents=dependents,
            )
        )
    return out


# ── Helpers ─────────────────────────────────────────────────────────


def _make_sampleable(opts: AnalyzeOptions) -> Callable[[str, str], bool]:
    """Build the classification gate from options (escape hatch + Phase-9 hook)."""

    excluded = opts.no_sample_columns

    def fn(table: str, column: str) -> bool:
        if column in excluded or f"{table}.{column}" in excluded:
            return False
        if opts.is_sampleable is not None:
            return opts.is_sampleable(table, column)
        return True

    return fn


def _safe_probe(call: Callable[..., Optional[float]], *args: object) -> Optional[float]:
    """Run a sampler probe, swallowing failures into ``None`` (resilience)."""
    try:
        return call(*args)
    except Exception as err:  # noqa: BLE001
        logger.warning("denorm_probe_failed", error=str(err))
        return None


def _dedupe(findings: list[DenormFinding]) -> list[DenormFinding]:
    """Keep the highest-confidence finding per ``(kind, table, columns)``."""
    best: dict[tuple[str, str, tuple[str, ...]], DenormFinding] = {}
    for f in findings:
        key = (f.kind, f.table, tuple(f.columns))
        prior = best.get(key)
        if prior is None or f.confidence > prior.confidence:
            best[key] = f
    return list(best.values())
