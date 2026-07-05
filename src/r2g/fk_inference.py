"""Foreign-key inference for schemas without declared constraints.

Snowflake does not enforce FK constraints and many Snowflake schemas
have none declared at all (PRD P6.6). Even a PostgreSQL dump or a
warehouse-landed table may have lost its referential metadata. This
module provides a *pure-Python* heuristic engine that suggests probable
FKs so the Mapping Studio / CLI can show them as "confirm to accept"
candidates without silently inventing graph topology.

Design
------

The inference engine, options, and the ``InferredForeignKey`` model are shared
with ``relational-schema-analyzer`` (RSA extracted them from r2g). This module is
a thin r2g-facing layer over that shared core:

- :func:`infer_foreign_keys` wraps RSA's engine and re-wraps its results as r2g's
  :class:`InferredForeignKey` (which adds :meth:`~InferredForeignKey.to_edge_definition`,
  the ArangoDB materialization that stays in r2g).
- The concrete value samplers subclass RSA's samplers to add
  :meth:`sample_values` (r2g's Phase-10c value-sampling probe, consumed by
  ``r2g.llm.sampling``). The FK-overlap ``__call__`` and the Phase-11
  denormalization probes are inherited unchanged.
- :func:`create_value_sampler` builds the r2g sampler subclasses for a source.

See ``docs/internal/DESIGN-rsa-compat-layer.md`` (Stage 2, steps 4–5).

The engine returns :class:`InferredForeignKey` objects sorted by ``confidence``
descending. The public API is intentionally small — callers that want to persist
accepted suggestions use the existing ``EdgeDefinition`` model (see
:meth:`InferredForeignKey.to_edge_definition`).
"""

from __future__ import annotations

from typing import Optional

# The heuristic engine, options, the InferredForeignKey model, and the concrete
# value samplers are shared with ``relational-schema-analyzer`` (RSA extracted them
# from r2g). r2g keeps only: a thin InferredForeignKey subclass that adds the
# ArangoDB ``to_edge_definition`` conversion, a wrapper that re-wraps RSA's results
# as that subclass, and sampler subclasses that add ``sample_values``. See
# ``docs/internal/DESIGN-rsa-compat-layer.md`` (Stage 2, steps 4–5).
from relational_schema_analyzer.fk_inference import CsvValueSampler as _RsaCsvValueSampler
from relational_schema_analyzer.fk_inference import InferenceMethod as InferenceMethod  # noqa: F401
from relational_schema_analyzer.fk_inference import InferenceOptions as InferenceOptions  # noqa: F401
from relational_schema_analyzer.fk_inference import InferredForeignKey as _RsaInferredForeignKey
from relational_schema_analyzer.fk_inference import MySQLValueSampler as _RsaMySQLValueSampler
from relational_schema_analyzer.fk_inference import PostgresValueSampler as _RsaPostgresValueSampler
from relational_schema_analyzer.fk_inference import Sampler as Sampler  # noqa: F401
from relational_schema_analyzer.fk_inference import SamplerResult as SamplerResult  # noqa: F401
from relational_schema_analyzer.fk_inference import SQLServerValueSampler as _RsaSQLServerValueSampler
from relational_schema_analyzer.fk_inference import infer_foreign_keys as _rsa_infer_foreign_keys

from r2g.log import get_logger
from r2g.types import EdgeDefinition, Schema

logger = get_logger(__name__)


# ── Public models ────────────────────────────────────────────────────


class InferredForeignKey(_RsaInferredForeignKey):
    """An FK candidate — RSA's shared model plus r2g's ArangoDB edge conversion.

    The inference *engine* (:func:`infer_foreign_keys` and its heuristics) lives in
    ``relational-schema-analyzer``. r2g subclasses RSA's ``InferredForeignKey`` only
    to add :meth:`to_edge_definition`, the ArangoDB-specific materialization that
    stays in r2g (RSA's relational analogue, ``to_foreign_key``, produces a
    declared :class:`~relational_schema_analyzer.types.ForeignKey` instead).
    """

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


# ── Entry point ─────────────────────────────────────────────────────


def infer_foreign_keys(
    schema: Schema,
    *,
    options: Optional[InferenceOptions] = None,
    sampler: Optional[Sampler] = None,
) -> list[InferredForeignKey]:
    """Return ranked FK candidates for ``schema``.

    Thin wrapper over the shared
    ``relational_schema_analyzer.fk_inference.infer_foreign_keys`` engine that
    re-wraps each result as r2g's :class:`InferredForeignKey` so callers keep the
    :meth:`to_edge_definition` convenience. r2g's ``Schema`` is an RSA
    ``PhysicalSchema`` subclass, so it is passed straight through; results are
    deduplicated, ranked by confidence, and filtered by ``options.min_confidence``
    exactly as the shared engine specifies.
    """
    return [
        InferredForeignKey.model_validate(ifk.model_dump())
        for ifk in _rsa_infer_foreign_keys(schema, options=options, sampler=sampler)
    ]


# ── Concrete value samplers ─────────────────────────────────────────
#
# The FK-overlap ``__call__`` and the Phase-11 denormalization probes
# (``distinct_ratio`` / ``group_single_valued`` / ``delimiter_rate``) are shared
# with RSA and inherited unchanged. r2g adds only ``sample_values`` — a bounded
# "return N distinct non-null values" probe used by ``r2g.llm.sampling`` for
# LLM-assisted ontology derivation, which RSA's samplers do not carry.


class PostgresValueSampler(_RsaPostgresValueSampler):
    """RSA's PostgreSQL FK/denorm sampler plus r2g's ``sample_values`` probe."""

    def sample_values(self, table: str, column: str, limit: int = 5) -> list:
        """Return up to ``limit`` distinct non-null values from a column (or [])."""
        n = max(1, int(limit))
        q = f"""
            SELECT DISTINCT "{column}" AS v
            FROM "{self.schema_name}"."{table}"
            WHERE "{column}" IS NOT NULL
            LIMIT %s
        """  # noqa: S608 - identifiers are quoted; schema is from the catalog
        try:
            conn = self._conn_lazy()
            with conn.cursor() as cur:
                cur.execute(q, (n,))
                return [r[0] for r in cur.fetchall()]
        except Exception as err:  # noqa: BLE001
            logger.warning("sample_values_failed", table=table, column=column, error=str(err))
            try:
                if self._conn is not None:
                    self._conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            return []


class MySQLValueSampler(_RsaMySQLValueSampler):
    """RSA's MySQL / MariaDB FK/denorm sampler plus r2g's ``sample_values`` probe."""

    def sample_values(self, table: str, column: str, limit: int = 5) -> list:
        """Return up to ``limit`` distinct non-null values from a column (or [])."""
        n = max(1, int(limit))
        db = self._qi(self.schema_name)
        q = f"""
            SELECT DISTINCT {self._qi(column)} AS v
            FROM {db}.{self._qi(table)}
            WHERE {self._qi(column)} IS NOT NULL
            LIMIT %s
        """  # noqa: S608 - identifiers are backtick-quoted; db is from the catalog
        try:
            conn = self._conn_lazy()
            with conn.cursor() as cur:
                cur.execute(q, (n,))
                return [r[0] for r in cur.fetchall()]
        except Exception as err:  # noqa: BLE001
            logger.warning("sample_values_failed", table=table, column=column, error=str(err))
            return []


class SQLServerValueSampler(_RsaSQLServerValueSampler):
    """RSA's SQL Server FK/denorm sampler plus r2g's ``sample_values`` probe."""

    def sample_values(self, table: str, column: str, limit: int = 5) -> list:
        """Return up to ``limit`` distinct non-null values from a column (or [])."""
        n = max(1, int(limit))
        s = self._qi(self.schema_name)
        q = f"""
            SELECT DISTINCT TOP (%s) {self._qi(column)} AS v
            FROM {s}.{self._qi(table)}
            WHERE {self._qi(column)} IS NOT NULL
        """  # noqa: S608 - identifiers are bracket-quoted; schema is from the catalog
        try:
            conn = self._conn_lazy()
            cur = conn.cursor()
            try:
                cur.execute(q, (n,))
                return [r[0] for r in cur.fetchall()]
            finally:
                cur.close()
        except Exception as err:  # noqa: BLE001
            logger.warning("sample_values_failed", table=table, column=column, error=str(err))
            return []


class CsvValueSampler(_RsaCsvValueSampler):
    """RSA's CSV FK/denorm sampler plus r2g's ``sample_values`` probe."""

    def sample_values(self, table: str, column: str, limit: int = 5) -> list:
        """Return up to ``limit`` distinct non-null values from a column (or [])."""
        import polars as pl

        frame = self._read_columns(table, [column])
        if frame is None or not frame.columns:
            return []
        name = frame.columns[0]
        series = frame.get_column(name).filter(
            pl.col(name).is_not_null() & (pl.col(name) != "")
        )
        if series.len() == 0:
            return []
        return series.unique(maintain_order=True).head(max(1, int(limit))).to_list()


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
    MySQL / MariaDB → :class:`MySQLValueSampler`, SQL Server →
    :class:`SQLServerValueSampler`, CSV → :class:`CsvValueSampler`; any other
    type returns ``None`` (the caller should fall back to name-only inference).
    Connector / import errors are allowed to propagate so callers can decide
    whether to log-and-continue.
    """
    from r2g.connectors.base import (
        expand_env_vars,
        is_mysql,
        is_postgresql,
        is_sqlserver,
        normalize_source_type,
    )

    params = source_params or {}
    # Resolve $VAR credential references the same way create_source_connector does.
    connection_string = expand_env_vars(connection_string)
    if is_postgresql(source_type):
        return PostgresValueSampler(connection_string, schema_name=pg_schema, limit=limit)
    if is_mysql(source_type):
        return MySQLValueSampler(connection_string, schema_name=pg_schema, limit=limit)
    if is_sqlserver(source_type):
        return SQLServerValueSampler(connection_string, schema_name=pg_schema, limit=limit)
    if normalize_source_type(source_type) == "csv":
        return CsvValueSampler(
            connection_string,
            delimiter=params.get("delimiter", ","),
            has_header=bool(params.get("has_header", True)),
            limit=limit,
        )
    return None
