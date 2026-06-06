"""Foreign-key inference for schemas without declared constraints.

Snowflake does not enforce FK constraints and many Snowflake schemas
have none declared at all (PRD P6.6). Even a PostgreSQL dump or a
warehouse-landed table may have lost its referential metadata. This
module provides a *pure-Python* heuristic engine that suggests probable
FKs so the Mapping Studio / CLI can show them as "confirm to accept"
candidates without silently inventing graph topology.

Design
------

The inference engine is deliberately sampler-pluggable and
source-agnostic:

- :func:`infer_foreign_keys` takes a :class:`r2g.types.Schema` and an
  optional ``sampler`` callable. Without a sampler it returns purely
  name-driven suggestions. With a sampler, it augments every candidate
  with a value-overlap score that either boosts or vetoes it.
- The heuristic runs in two passes: single-column candidates first
  (the common ``{prefix}_id`` → ``{prefix}s.id`` case), then a
  composite pass that groups single-column candidates with the same
  local table → foreign table pair and aligns them by column order.
- Declared FKs already present on a table short-circuit the search for
  that exact column set. We never emit a suggestion that duplicates a
  declared constraint.

The engine returns :class:`InferredForeignKey` objects sorted by
``confidence`` descending. The public API is intentionally small —
callers that want to persist accepted suggestions use the existing
``EdgeDefinition`` model (see :meth:`InferredForeignKey.to_edge_definition`).

Heuristic details
-----------------

For every non-PK column ``c`` in every table ``T`` we consider these
patterns:

1. ``{prefix}_id`` → foreign table is singular or plural of ``prefix``
   (``user_id`` → ``user`` or ``users``). The foreign column is the
   target table's primary key if it is a single column.
2. ``{prefix}id`` (no underscore, len > 3) → same, with a penalty.
3. ``{prefix}_{pkcol}`` → for tables with a non-``id`` PK name
   (``order_sku`` → ``orders.sku``).
4. Direct PK-name match across tables for non-generic PK names
   (``sku`` in one table and ``sku`` as PK of another; we never match
   bare ``id``).

Each match is filtered by JSON-level type compatibility
(``pg_type_to_json_type`` shared between PG and Snowflake). Confidence
starts at a pattern-specific base and is modulated by:

- ``+0.1`` when both columns use identical data-type strings.
- ``+0.15`` when the sampler reports overlap ≥ 0.9 (strong signal).
- ``+0.05`` when overlap ≥ 0.5.
- ``-0.25`` when overlap is 0 (hard veto unless caller disables).
- ``-0.1`` per non-nullable column that looks like it points at a
  nullable PK (usually a modelling mistake, not a real FK).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Literal, Optional

from pydantic import BaseModel, Field

from r2g.config import pg_type_to_json_type
from r2g.log import get_logger
from r2g.types import Column, EdgeDefinition, Schema, Table

logger = get_logger(__name__)


# ── Public models ────────────────────────────────────────────────────


InferenceMethod = Literal["name_suffix", "name_no_underscore", "pk_name_match", "composite"]


class InferredForeignKey(BaseModel):
    """A single FK candidate produced by :func:`infer_foreign_keys`."""

    table: str
    columns: list[str]
    foreign_table: str
    foreign_columns: list[str]
    confidence: float = Field(ge=0.0, le=1.0)
    method: InferenceMethod
    evidence: list[str] = Field(default_factory=list)

    def to_edge_definition(self, edge_collection: Optional[str] = None) -> EdgeDefinition:
        """Convert the suggestion to an :class:`EdgeDefinition` for mapping.

        The default edge-collection name follows R2G's existing
        convention (``<table>_to_<foreign_table>``) so inferred edges
        live alongside declared ones with a consistent namespace.
        """
        name = edge_collection or f"{self.table}_to_{self.foreign_table}"
        return EdgeDefinition(
            edge_collection=name,
            from_collection=self.table,
            to_collection=self.foreign_table,
            from_fields=list(self.columns),
            to_fields=list(self.foreign_columns),
        )


# ── Options ─────────────────────────────────────────────────────────


@dataclass
class InferenceOptions:
    """Knobs that shape what the engine considers / returns."""

    min_confidence: float = 0.4
    generic_pk_names: frozenset[str] = field(
        default_factory=lambda: frozenset({"id", "uuid", "pk", "key"})
    )
    max_candidates_per_column: int = 3
    allow_composite: bool = True
    sample_overlap: bool = False
    overlap_veto_on_zero: bool = True


# ── Sampler protocol ────────────────────────────────────────────────


SamplerResult = Optional[float]
"""Sampler callables return an overlap ratio in [0, 1], or ``None`` to
indicate "no data / couldn't evaluate" (the engine then skips the
overlap signal rather than treating it as a veto)."""


Sampler = Callable[[str, str, str, str], SamplerResult]
"""``sampler(local_table, local_column, foreign_table, foreign_column)``"""


# ── Entry point ─────────────────────────────────────────────────────


def infer_foreign_keys(
    schema: Schema,
    *,
    options: Optional[InferenceOptions] = None,
    sampler: Optional[Sampler] = None,
) -> list[InferredForeignKey]:
    """Return ranked FK candidates for ``schema``.

    See module docstring for the heuristic. Results are deduplicated
    (one entry per ``(table, tuple(columns), foreign_table)`` triple),
    sorted by confidence descending, and filtered by
    ``options.min_confidence``.
    """
    opts = options or InferenceOptions()

    pk_index = _build_pk_index(schema, generic_pk_names=opts.generic_pk_names)
    declared_index = _build_declared_fk_index(schema)

    single: list[InferredForeignKey] = []
    for table_name, table in schema.tables.items():
        pk_set = set(table.primary_key)
        for col in table.columns:
            if col.name in pk_set and len(table.primary_key) == 1:
                # Skip single-column PKs — they are the referenced side,
                # not a FK origin (a table's lone PK very rarely *also*
                # points somewhere else).
                continue
            if _column_is_covered_by_declared_fk(col.name, table_name, declared_index):
                continue
            single.extend(
                _candidates_for_column(
                    schema,
                    table_name,
                    col,
                    pk_index,
                    opts,
                )
            )

    # Composite pass: group single-column candidates sharing (table, foreign_table)
    composite: list[InferredForeignKey] = []
    if opts.allow_composite:
        composite = _find_composite_candidates(schema, single, declared_index)

    # Sampler pass: adjust confidence on every candidate we still have.
    all_candidates = single + composite
    if sampler is not None and opts.sample_overlap:
        all_candidates = [
            _apply_sampler(c, sampler, opts) for c in all_candidates
        ]
        all_candidates = [c for c in all_candidates if c is not None]  # type: ignore[list-item]

    # Dedup + rank + filter.
    deduped = _dedupe(all_candidates)
    ranked = sorted(deduped, key=lambda c: c.confidence, reverse=True)
    return [c for c in ranked if c.confidence >= opts.min_confidence]


# ── Internal helpers ────────────────────────────────────────────────


def _build_pk_index(
    schema: Schema, *, generic_pk_names: frozenset[str]
) -> dict[str, list[tuple[str, list[str]]]]:
    """Index tables by their PK column name(s).

    Returns ``{pk_col_name_lower: [(table_name, pk_col_names), ...]}``.
    Tables with no PK, multi-column PKs, or generic PKs still enter the
    index under their first PK column; generic names are consulted only
    via the ``{prefix}_id`` pattern (never matched bare).
    """
    idx: dict[str, list[tuple[str, list[str]]]] = {}
    for table_name, table in schema.tables.items():
        if not table.primary_key:
            continue
        first = table.primary_key[0].lower()
        idx.setdefault(first, []).append((table_name, list(table.primary_key)))
    return idx


def _build_declared_fk_index(schema: Schema) -> dict[str, set[tuple[str, ...]]]:
    """Index ``{table_name: {tuple(fk_columns), ...}}`` for existing FKs."""
    idx: dict[str, set[tuple[str, ...]]] = {}
    for table_name, table in schema.tables.items():
        idx[table_name] = {tuple(sorted(fk.columns)) for fk in table.foreign_keys}
    return idx


def _column_is_covered_by_declared_fk(
    col: str, table: str, declared_index: dict[str, set[tuple[str, ...]]]
) -> bool:
    fks = declared_index.get(table, set())
    return any(col in cols for cols in fks)


def _candidates_for_column(
    schema: Schema,
    table_name: str,
    col: Column,
    pk_index: dict[str, list[tuple[str, list[str]]]],
    opts: InferenceOptions,
) -> list[InferredForeignKey]:
    """Produce single-column FK candidates that column ``col`` could be."""
    candidates: list[InferredForeignKey] = []
    local_lower = col.name.lower()

    # Pattern 1 / 2: {prefix}_id, {prefix}id, {prefix}_{pkcol}
    prefix_matches = _split_prefix(local_lower)
    for prefix, suffix, method, base_conf in prefix_matches:
        for foreign_table, pk_cols in _candidate_tables_for_prefix(
            schema, prefix, opts.generic_pk_names
        ):
            if foreign_table == table_name:
                continue
            if len(pk_cols) != 1:
                # Composite PKs need the composite pass, not a single-column match.
                continue
            foreign_col = pk_cols[0]
            if suffix and suffix != foreign_col.lower():
                # e.g. pattern `{prefix}_{pkcol}` wants suffix == pk name.
                continue
            cand = _make_candidate(
                schema,
                table_name,
                [col.name],
                foreign_table,
                [foreign_col],
                method=method,
                base_confidence=base_conf,
                evidence=[f"name pattern '{col.name}' → {foreign_table}.{foreign_col}"],
            )
            if cand is not None:
                candidates.append(cand)

    # Pattern 4: direct PK-name match (non-generic).
    if local_lower not in opts.generic_pk_names:
        for foreign_table, pk_cols in pk_index.get(local_lower, []):
            if foreign_table == table_name:
                continue
            if len(pk_cols) != 1:
                continue
            cand = _make_candidate(
                schema,
                table_name,
                [col.name],
                foreign_table,
                pk_cols,
                method="pk_name_match",
                base_confidence=0.55,
                evidence=[
                    f"column '{col.name}' matches PK of '{foreign_table}' "
                    f"(non-generic name)"
                ],
            )
            if cand is not None:
                candidates.append(cand)

    # Trim to top-N per column.
    candidates.sort(key=lambda c: c.confidence, reverse=True)
    return candidates[: opts.max_candidates_per_column]


def _split_prefix(col_lower: str) -> list[tuple[str, str, InferenceMethod, float]]:
    """Return ``(prefix, required_suffix, method, base_confidence)`` tuples
    a column name could resolve to.

    The ``required_suffix`` is used by pattern 3 to insist on a specific
    PK column name; otherwise it is ``""`` which accepts any single-col
    PK.
    """
    out: list[tuple[str, str, InferenceMethod, float]] = []
    # Pattern 1: ends with _id
    if col_lower.endswith("_id") and len(col_lower) > 3:
        out.append((col_lower[:-3], "", "name_suffix", 0.75))
    # Pattern 3: {prefix}_{suffix} where suffix looks like a PK name
    if "_" in col_lower:
        prefix, _, suffix = col_lower.rpartition("_")
        if prefix and suffix and suffix not in ("", "id") and len(suffix) >= 2:
            out.append((prefix, suffix, "name_suffix", 0.6))
    # Pattern 2: ends with 'id' but no underscore (userid, orderid)
    if (
        col_lower.endswith("id")
        and not col_lower.endswith("_id")
        and len(col_lower) > 3
        and col_lower != "uuid"
    ):
        out.append((col_lower[:-2], "", "name_no_underscore", 0.45))
    return out


def _candidate_tables_for_prefix(
    schema: Schema, prefix: str, generic_pk_names: frozenset[str]
) -> list[tuple[str, list[str]]]:
    """Return ``(table_name, pk_cols)`` for tables whose name matches ``prefix``
    in singular/plural form.
    """
    if not prefix:
        return []
    candidates: list[tuple[str, list[str]]] = []
    target_names = {prefix, _pluralize(prefix), _singularize(prefix)}
    for table_name, table in schema.tables.items():
        if table_name.lower() in target_names and table.primary_key:
            pk_cols = list(table.primary_key)
            if len(pk_cols) == 1 and pk_cols[0].lower() in generic_pk_names:
                candidates.append((table_name, pk_cols))
            elif len(pk_cols) == 1:
                candidates.append((table_name, pk_cols))
    return candidates


from r2g.naming import pluralize as _pluralize  # noqa: E402
from r2g.naming import singularize as _singularize  # noqa: E402


def _make_candidate(
    schema: Schema,
    table: str,
    columns: list[str],
    foreign_table: str,
    foreign_columns: list[str],
    *,
    method: InferenceMethod,
    base_confidence: float,
    evidence: list[str],
) -> Optional[InferredForeignKey]:
    """Validate type compatibility and assemble an :class:`InferredForeignKey`.

    Returns ``None`` if the types are incompatible (we never suggest an
    FK from a ``boolean`` column to an ``integer`` PK, for example).
    """
    local_tbl = schema.tables.get(table)
    foreign_tbl = schema.tables.get(foreign_table)
    if local_tbl is None or foreign_tbl is None:
        return None

    confidence = base_confidence
    details = list(evidence)

    for lcol_name, fcol_name in zip(columns, foreign_columns):
        lcol = _find_col(local_tbl, lcol_name)
        fcol = _find_col(foreign_tbl, fcol_name)
        if lcol is None or fcol is None:
            return None
        if not _types_compatible(lcol, fcol):
            return None
        if lcol.data_type.strip().lower() == fcol.data_type.strip().lower():
            confidence += 0.1
            details.append(f"identical data_type '{lcol.data_type}'")
        if (not lcol.is_nullable) and fcol.is_nullable:
            # Unusual: a non-nullable child pointing at a nullable-PK parent.
            confidence -= 0.1
            details.append(
                f"'{table}.{lcol_name}' is NOT NULL but '{foreign_table}.{fcol_name}' is nullable"
            )

    confidence = max(0.0, min(1.0, confidence))
    return InferredForeignKey(
        table=table,
        columns=list(columns),
        foreign_table=foreign_table,
        foreign_columns=list(foreign_columns),
        confidence=round(confidence, 3),
        method=method,
        evidence=details,
    )


def _find_col(table: Table, name: str) -> Optional[Column]:
    for c in table.columns:
        if c.name == name:
            return c
    return None


_COMPATIBLE_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({"integer", "float"}),  # numeric join is common across NUMBER/int
    frozenset({"string"}),
    frozenset({"boolean"}),
)


def _types_compatible(a: Column, b: Column) -> bool:
    aj = pg_type_to_json_type(a.data_type)
    bj = pg_type_to_json_type(b.data_type)
    if aj == bj:
        return True
    for group in _COMPATIBLE_GROUPS:
        if aj in group and bj in group:
            return True
    return False


def _find_composite_candidates(
    schema: Schema,
    single_candidates: list[InferredForeignKey],  # kept for future use (e.g. evidence merging)
    declared_index: dict[str, set[tuple[str, ...]]],
) -> list[InferredForeignKey]:
    """Directly scan the schema for composite-PK FK candidates.

    For every foreign table ``F`` with a multi-column primary key
    ``(k1, …, kn)``, find local tables ``L`` that contain every ``ki``
    as a non-PK column with a type compatible with ``F.ki``. Emit a
    composite suggestion preserving ``F``'s PK column order.

    This is intentionally independent of the single-column pass: the
    child column names don't have to match the parent table's name
    (a common modelling style for junction tables whose name is
    unrelated to its parents, e.g. ``enrollments`` referencing
    ``course_offerings``).
    """
    del single_candidates  # parameter reserved for future evidence merging
    out: list[InferredForeignKey] = []

    for foreign_table, foreign in schema.tables.items():
        if len(foreign.primary_key) < 2:
            continue
        pk_cols = [_find_col(foreign, k) for k in foreign.primary_key]
        if any(pc is None for pc in pk_cols):
            continue

        for local_table, local in schema.tables.items():
            if local_table == foreign_table:
                continue
            local_pk_set = set(local.primary_key)
            # Require every PK column name to be present in the local
            # table as a non-PK-only column (it may still participate
            # in the local PK — that's fine, e.g. a junction table
            # whose own PK is the composite FK).
            matched: list[Column] = []
            ok = True
            for pc in pk_cols:
                lcol = _find_col(local, pc.name)  # type: ignore[union-attr]
                if lcol is None:
                    ok = False
                    break
                if not _types_compatible(lcol, pc):  # type: ignore[arg-type]
                    ok = False
                    break
                matched.append(lcol)
            if not ok:
                continue

            # Skip if the local table's single-column PK is exactly one
            # of the pieces (rare but would be a self-inconsistent match).
            if len(local.primary_key) == 1 and local.primary_key[0] in {pc.name for pc in pk_cols}:  # type: ignore[union-attr]
                # Only treat as composite if *all* pieces are present,
                # which the loop above already verified.
                pass

            columns = [pc.name for pc in pk_cols]  # type: ignore[union-attr]
            foreign_columns = list(foreign.primary_key)
            if tuple(sorted(columns)) in declared_index.get(local_table, set()):
                continue

            # Base confidence: 0.7, plus +0.05 per component column that
            # is non-nullable on both sides (strong signal).
            base = 0.7
            nn_bonus = sum(
                0.05
                for lc, pc in zip(matched, pk_cols)
                if (not lc.is_nullable) and (not pc.is_nullable)  # type: ignore[union-attr]
            )
            same_type_bonus = sum(
                0.05
                for lc, pc in zip(matched, pk_cols)
                if lc.data_type.strip().lower() == pc.data_type.strip().lower()  # type: ignore[union-attr]
            )
            # Penalize matches where the local table also has a plain
            # "id" column that looks like it already points elsewhere —
            # the composite is still a valid suggestion but less sure.
            confidence = min(1.0, base + nn_bonus + same_type_bonus)
            # Exclude local tables that are obviously unrelated (no
            # shared column names outside the composite pieces) only if
            # confidence ended up low — we keep clean composite matches
            # regardless.
            if local_pk_set == set(columns):
                confidence = min(1.0, confidence + 0.05)

            out.append(
                InferredForeignKey(
                    table=local_table,
                    columns=columns,
                    foreign_table=foreign_table,
                    foreign_columns=foreign_columns,
                    confidence=round(confidence, 3),
                    method="composite",
                    evidence=[
                        f"composite match on PK of '{foreign_table}': "
                        f"({', '.join(foreign_columns)})"
                    ],
                )
            )
    return out


def _apply_sampler(
    c: InferredForeignKey, sampler: Sampler, opts: InferenceOptions
) -> Optional[InferredForeignKey]:
    """Invoke the sampler for each (local, foreign) column pair and fold
    the result into the candidate's confidence score.

    Per-column overlaps are averaged. A single zero result vetoes the
    whole candidate when ``opts.overlap_veto_on_zero`` is set.
    """
    scores: list[float] = []
    any_zero = False
    for lcol, fcol in zip(c.columns, c.foreign_columns):
        try:
            score = sampler(c.table, lcol, c.foreign_table, fcol)
        except Exception as err:  # noqa: BLE001
            logger.warning(
                "fk_sampler_failed",
                table=c.table,
                column=lcol,
                foreign_table=c.foreign_table,
                error=str(err),
            )
            return c  # keep candidate; sampler noise shouldn't drop it
        if score is None:
            continue
        if score <= 0.0:
            any_zero = True
        scores.append(score)

    if not scores:
        return c

    avg = sum(scores) / len(scores)
    evidence = list(c.evidence) + [f"value overlap avg={avg:.2f} ({len(scores)} cols sampled)"]

    if any_zero and opts.overlap_veto_on_zero:
        return None

    bump = 0.0
    if avg >= 0.9:
        bump = 0.15
    elif avg >= 0.5:
        bump = 0.05
    elif avg <= 0.0:
        bump = -0.25

    new_conf = max(0.0, min(1.0, c.confidence + bump))
    return c.model_copy(update={"confidence": round(new_conf, 3), "evidence": evidence})


def _dedupe(candidates: list[InferredForeignKey]) -> list[InferredForeignKey]:
    """Keep the highest-confidence candidate per ``(table, columns, foreign_table)``."""
    best: dict[tuple[str, tuple[str, ...], str], InferredForeignKey] = {}
    for c in candidates:
        key = (c.table, tuple(c.columns), c.foreign_table)
        prior = best.get(key)
        if prior is None or c.confidence > prior.confidence:
            best[key] = c
    return list(best.values())


# ── Concrete PostgreSQL value sampler ───────────────────────────────


class PostgresValueSampler:
    """Sampler that computes FK value-overlap ratios via PostgreSQL.

    We use one bounded query per (local column, foreign column) pair::

        SELECT COUNT(DISTINCT l.col)::float
             / GREATEST(COUNT(DISTINCT l.col), 1)
        FROM <local_sample> l
        LEFT JOIN <foreign_sample> f ON l.col = f.pkcol;

    To keep runtime bounded on large tables we materialize small
    ``LIMIT`` CTEs for both sides (default 10k rows). This is a
    *statistical* signal, not a proof — the engine treats it as one
    input alongside the name-based score.

    Usage::

        sampler = PostgresValueSampler(conn_str, schema_name="public", limit=10_000)
        infer_foreign_keys(schema, options=InferenceOptions(sample_overlap=True),
                           sampler=sampler)

    The sampler is resilient: any exception from psycopg gets caught
    and surfaced as ``None`` so the name-based score still wins. Call
    :meth:`close` to release the connection.
    """

    def __init__(
        self,
        connection_string: str,
        *,
        schema_name: str = "public",
        limit: int = 10_000,
    ) -> None:
        self.connection_string = connection_string
        self.schema_name = schema_name
        self.limit = max(100, int(limit))
        self._conn = None

    def _conn_lazy(self):
        if self._conn is None:
            import psycopg

            self._conn = psycopg.connect(self.connection_string)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def __enter__(self) -> "PostgresValueSampler":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __call__(
        self,
        local_table: str,
        local_column: str,
        foreign_table: str,
        foreign_column: str,
    ) -> SamplerResult:
        """Return the fraction of distinct local values present in the
        foreign column, or ``None`` if the query failed."""
        try:
            conn = self._conn_lazy()
        except Exception as err:  # noqa: BLE001
            logger.warning("fk_sampler_connect_failed", error=str(err))
            return None

        q = f"""
            WITH l AS (
                SELECT DISTINCT "{local_column}" AS v
                FROM "{self.schema_name}"."{local_table}"
                WHERE "{local_column}" IS NOT NULL
                LIMIT %s
            ),
            f AS (
                SELECT DISTINCT "{foreign_column}" AS v
                FROM "{self.schema_name}"."{foreign_table}"
                LIMIT %s
            )
            SELECT
                COUNT(*) FILTER (WHERE f.v IS NOT NULL)::float
                    / GREATEST(COUNT(*), 1)::float AS overlap
            FROM l
            LEFT JOIN f ON l.v = f.v
        """  # noqa: S608 - identifiers are quoted with " and schema is from catalog
        try:
            with conn.cursor() as cur:
                cur.execute(q, (self.limit, self.limit))
                row = cur.fetchone()
                if not row:
                    return None
                value = row[0]
                if value is None:
                    return None
                return float(value)
        except Exception as err:  # noqa: BLE001
            logger.warning(
                "fk_sampler_query_failed",
                local=f"{local_table}.{local_column}",
                foreign=f"{foreign_table}.{foreign_column}",
                error=str(err),
            )
            # Roll back the aborted transaction so future queries succeed.
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            return None


# ── Concrete CSV value sampler ──────────────────────────────────────


class CsvValueSampler:
    """Sampler that computes FK value-overlap ratios across CSV files.

    A CSV source is a directory of files (one per table, filename stem =
    table name). For each ``(local_column, foreign_column)`` pair we read
    just those two columns (bounded by ``limit`` rows) and return the
    fraction of distinct local values that also appear in the foreign
    column — the same statistic :class:`PostgresValueSampler` computes
    with a ``LEFT JOIN``.

    Values are compared as *raw text* (columns are read with type
    inference disabled) so that ``1`` and ``1.0`` — which Polars might
    otherwise type as int on one side and float on the other — still
    match on their textual token, which is what actually joins in the
    file.

    The sampler is resilient: any read failure (missing file, unreadable
    column, Polars error) is logged and surfaced as ``None`` so the
    name-based score still wins. ``close`` is a no-op; the context-manager
    protocol is provided for parity with :class:`PostgresValueSampler`.
    """

    def __init__(
        self,
        connection_string: str,
        *,
        delimiter: str = ",",
        has_header: bool = True,
        limit: int = 10_000,
    ) -> None:
        from pathlib import Path

        self.connection_string = connection_string
        self.delimiter = delimiter
        self.has_header = has_header
        self.limit = max(100, int(limit))
        self.directory = Path(connection_string).expanduser()

    def close(self) -> None:
        return None

    def __enter__(self) -> "CsvValueSampler":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _resolve(self, table: str):
        from r2g.connectors.csv_source import resolve_csv_table_path

        return resolve_csv_table_path(self.directory, table)

    def _distinct_text_values(self, table: str, column: str) -> Optional[set[str]]:
        """Return the set of distinct, non-empty textual values for a column,
        or ``None`` if the file/column could not be read."""
        import polars as pl

        path = self._resolve(table)
        if path is None:
            return None
        try:
            frame = pl.read_csv(
                str(path),
                separator=self.delimiter,
                has_header=self.has_header,
                columns=[column],
                n_rows=self.limit,
                infer_schema_length=0,  # read everything as Utf8 (raw text)
            )
        except Exception as err:  # noqa: BLE001
            logger.warning(
                "csv_fk_sampler_read_failed",
                table=table,
                column=column,
                error=str(err),
            )
            return None
        if not frame.columns:
            return None
        series = frame.get_column(frame.columns[0])
        return {
            v for v in series.to_list() if v is not None and str(v) != ""
        }

    def __call__(
        self,
        local_table: str,
        local_column: str,
        foreign_table: str,
        foreign_column: str,
    ) -> SamplerResult:
        """Return the fraction of distinct local values present in the
        foreign column, or ``None`` if either side could not be read."""
        local_vals = self._distinct_text_values(local_table, local_column)
        if not local_vals:
            return None
        foreign_vals = self._distinct_text_values(foreign_table, foreign_column)
        if foreign_vals is None:
            return None
        overlap = len(local_vals & foreign_vals) / len(local_vals)
        return float(overlap)


def create_value_sampler(
    source_type: str | None,
    connection_string: str,
    *,
    pg_schema: str = "public",
    source_params: dict | None = None,
    limit: int = 10_000,
):
    """Build a value-overlap sampler for a source, or ``None`` if unsupported.

    PostgreSQL (incl. ``postgres`` / ``pg`` aliases) → :class:`PostgresValueSampler`,
    CSV → :class:`CsvValueSampler`; any other type returns ``None`` (the caller
    should fall back to name-only inference). Connector / import errors are
    allowed to propagate so callers can decide whether to log-and-continue.
    """
    from r2g.connectors.base import is_postgresql, normalize_source_type

    params = source_params or {}
    if is_postgresql(source_type):
        return PostgresValueSampler(connection_string, schema_name=pg_schema, limit=limit)
    if normalize_source_type(source_type) == "csv":
        return CsvValueSampler(
            connection_string,
            delimiter=params.get("delimiter", ","),
            has_header=bool(params.get("has_header", True)),
            limit=limit,
        )
    return None
