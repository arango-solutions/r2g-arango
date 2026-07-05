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

from typing import Optional

# The heuristic engine, options, and the InferredForeignKey model are shared with
# ``relational-schema-analyzer`` (RSA extracted them from r2g). r2g keeps only a
# thin subclass that adds the ArangoDB ``to_edge_definition`` conversion, and a
# wrapper that re-wraps RSA's results as that subclass. See
# ``docs/internal/DESIGN-rsa-compat-layer.md`` (Stage 2, step 4).
from relational_schema_analyzer.fk_inference import InferenceMethod as InferenceMethod  # noqa: F401
from relational_schema_analyzer.fk_inference import InferenceOptions as InferenceOptions  # noqa: F401
from relational_schema_analyzer.fk_inference import InferredForeignKey as _RsaInferredForeignKey
from relational_schema_analyzer.fk_inference import Sampler as Sampler  # noqa: F401
from relational_schema_analyzer.fk_inference import SamplerResult as SamplerResult
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

    # ── Denormalization probes (PRD Phase 11) ──────────────────────

    def _scalar(self, query: str, params: tuple) -> SamplerResult:
        """Run a bounded scalar query, returning its float value or ``None``."""
        try:
            conn = self._conn_lazy()
        except Exception as err:  # noqa: BLE001
            logger.warning("denorm_sampler_connect_failed", error=str(err))
            return None
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
            if not row or row[0] is None:
                return None
            return float(row[0])
        except Exception as err:  # noqa: BLE001
            logger.warning("denorm_sampler_query_failed", error=str(err))
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            return None

    def distinct_ratio(self, table: str, column: str) -> SamplerResult:
        q = f"""
            WITH s AS (
                SELECT "{column}" AS v
                FROM "{self.schema_name}"."{table}"
                WHERE "{column}" IS NOT NULL
                LIMIT %s
            )
            SELECT COUNT(DISTINCT v)::float / GREATEST(COUNT(*), 1)::float FROM s
        """  # noqa: S608 - identifiers are quoted; schema is from the catalog
        return self._scalar(q, (self.limit,))

    def group_single_valued(
        self, table: str, determinant_columns: list[str], dependent_column: str
    ) -> SamplerResult:
        det = ", ".join(f'"{c}"' for c in determinant_columns)
        not_null = " AND ".join(f'"{c}" IS NOT NULL' for c in determinant_columns)
        q = f"""
            WITH s AS (
                SELECT {det}, "{dependent_column}" AS dep
                FROM "{self.schema_name}"."{table}"
                LIMIT %s
            ),
            g AS (
                SELECT {det}, COUNT(DISTINCT dep) AS dcount
                FROM s
                WHERE {not_null}
                GROUP BY {det}
            )
            SELECT COALESCE(AVG(CASE WHEN dcount <= 1 THEN 1.0 ELSE 0.0 END), 0)::float
            FROM g
        """  # noqa: S608 - identifiers are quoted; schema is from the catalog
        return self._scalar(q, (self.limit,))

    def delimiter_rate(self, table: str, column: str, delimiter: str) -> SamplerResult:
        q = f"""
            WITH s AS (
                SELECT "{column}" AS v
                FROM "{self.schema_name}"."{table}"
                WHERE "{column}" IS NOT NULL
                LIMIT %s
            )
            SELECT COALESCE(AVG(CASE WHEN strpos(v, %s) > 0 THEN 1.0 ELSE 0.0 END), 0)::float
            FROM s
        """  # noqa: S608 - identifiers are quoted; schema is from the catalog
        return self._scalar(q, (self.limit, delimiter))

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


# ── Concrete MySQL value sampler ────────────────────────────────────


class MySQLValueSampler:
    """Sampler that computes FK value-overlap ratios via MySQL / MariaDB.

    The MySQL analog of :class:`PostgresValueSampler`. MySQL has no
    ``FILTER (WHERE …)`` aggregate, so the overlap fraction is computed with
    ``SUM(CASE WHEN … )`` over a ``LEFT JOIN`` of two bounded, distinct-valued
    derived tables (default 10k rows per side).

    The database to query is taken from the connection string's path
    component; a non-default ``schema_name`` overrides it. The sampler is
    resilient: any driver error is logged and surfaced as ``None`` so the
    name-based score still wins. Call :meth:`close` to release the connection.
    """

    def __init__(
        self,
        connection_string: str,
        *,
        schema_name: str = "",
        limit: int = 10_000,
    ) -> None:
        from r2g.connectors.mysql import _DEFAULT_SCHEMA_SENTINELS, _parse_mysql_url

        self.connection_string = connection_string
        self.limit = max(100, int(limit))
        self._connect_params = _parse_mysql_url(connection_string)
        if schema_name in _DEFAULT_SCHEMA_SENTINELS:
            self.schema_name = self._connect_params["database"]
        else:
            self.schema_name = schema_name
            self._connect_params["database"] = schema_name
        self._conn = None

    def _conn_lazy(self):
        if self._conn is None:
            from r2g.connectors.mysql import _load_pymysql

            pymysql = _load_pymysql()
            self._conn = pymysql.connect(**self._connect_params)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def __enter__(self) -> "MySQLValueSampler":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _qi(self, name: str) -> str:
        return "`" + name.replace("`", "``") + "`"

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

        db = self._qi(self.schema_name)
        q = f"""
            SELECT SUM(CASE WHEN f.v IS NOT NULL THEN 1 ELSE 0 END)
                       / GREATEST(COUNT(*), 1) AS overlap
            FROM (
                SELECT DISTINCT {self._qi(local_column)} AS v
                FROM {db}.{self._qi(local_table)}
                WHERE {self._qi(local_column)} IS NOT NULL
                LIMIT %s
            ) l
            LEFT JOIN (
                SELECT DISTINCT {self._qi(foreign_column)} AS v
                FROM {db}.{self._qi(foreign_table)}
                LIMIT %s
            ) f ON l.v = f.v
        """  # noqa: S608 - identifiers are backtick-quoted; db is from the catalog
        try:
            with conn.cursor() as cur:
                cur.execute(q, (self.limit, self.limit))
                row = cur.fetchone()
                if not row or row[0] is None:
                    return None
                return float(row[0])
        except Exception as err:  # noqa: BLE001
            logger.warning(
                "fk_sampler_query_failed",
                local=f"{local_table}.{local_column}",
                foreign=f"{foreign_table}.{foreign_column}",
                error=str(err),
            )
            return None

    # ── Denormalization probes (PRD Phase 11) ──────────────────────

    def _scalar(self, query: str, params: tuple) -> SamplerResult:
        try:
            conn = self._conn_lazy()
        except Exception as err:  # noqa: BLE001
            logger.warning("denorm_sampler_connect_failed", error=str(err))
            return None
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
            if not row or row[0] is None:
                return None
            return float(row[0])
        except Exception as err:  # noqa: BLE001
            logger.warning("denorm_sampler_query_failed", error=str(err))
            return None

    def distinct_ratio(self, table: str, column: str) -> SamplerResult:
        db = self._qi(self.schema_name)
        q = f"""
            SELECT COUNT(DISTINCT v) / GREATEST(COUNT(*), 1) AS ratio
            FROM (
                SELECT {self._qi(column)} AS v
                FROM {db}.{self._qi(table)}
                WHERE {self._qi(column)} IS NOT NULL
                LIMIT %s
            ) s
        """  # noqa: S608 - identifiers are backtick-quoted; db is from the catalog
        return self._scalar(q, (self.limit,))

    def group_single_valued(
        self, table: str, determinant_columns: list[str], dependent_column: str
    ) -> SamplerResult:
        db = self._qi(self.schema_name)
        det = ", ".join(self._qi(c) for c in determinant_columns)
        not_null = " AND ".join(f"{self._qi(c)} IS NOT NULL" for c in determinant_columns)
        q = f"""
            SELECT AVG(CASE WHEN dcount <= 1 THEN 1.0 ELSE 0.0 END) AS frac
            FROM (
                SELECT COUNT(DISTINCT dep) AS dcount
                FROM (
                    SELECT {det}, {self._qi(dependent_column)} AS dep
                    FROM {db}.{self._qi(table)}
                    LIMIT %s
                ) s
                WHERE {not_null}
                GROUP BY {det}
            ) g
        """  # noqa: S608 - identifiers are backtick-quoted; db is from the catalog
        return self._scalar(q, (self.limit,))

    def delimiter_rate(self, table: str, column: str, delimiter: str) -> SamplerResult:
        db = self._qi(self.schema_name)
        q = f"""
            SELECT AVG(CASE WHEN LOCATE(%s, v) > 0 THEN 1.0 ELSE 0.0 END) AS rate
            FROM (
                SELECT {self._qi(column)} AS v
                FROM {db}.{self._qi(table)}
                WHERE {self._qi(column)} IS NOT NULL
                LIMIT %s
            ) s
        """  # noqa: S608 - identifiers are backtick-quoted; db is from the catalog
        return self._scalar(q, (delimiter, self.limit))

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


# ── Concrete SQL Server value sampler ───────────────────────────────


class SQLServerValueSampler:
    """Sampler that computes FK value-overlap ratios via Microsoft SQL Server.

    The SQL Server analog of :class:`MySQLValueSampler`, using T-SQL ``TOP (n)``
    (no ``LIMIT``), bracket-quoted identifiers, and ``CAST(... AS FLOAT)`` for
    the overlap fraction. The schema namespace is taken from ``schema_name``
    (the historical ``public`` default folds to ``dbo``); the database comes
    from the connection string. Driver errors are logged and surfaced as
    ``None`` so the name-based score still wins.
    """

    def __init__(
        self,
        connection_string: str,
        *,
        schema_name: str = "dbo",
        limit: int = 10_000,
    ) -> None:
        from r2g.connectors.mssql import _DEFAULT_SCHEMA_SENTINELS, _parse_mssql_url

        self.connection_string = connection_string
        self.limit = max(100, int(limit))
        self._connect_params = _parse_mssql_url(connection_string)
        self.schema_name = "dbo" if schema_name in _DEFAULT_SCHEMA_SENTINELS else schema_name
        self._conn = None

    def _conn_lazy(self):
        if self._conn is None:
            from r2g.connectors.mssql import _load_pymssql

            pymssql = _load_pymssql()
            self._conn = pymssql.connect(**self._connect_params)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def __enter__(self) -> "SQLServerValueSampler":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _qi(self, name: str) -> str:
        return "[" + name.replace("]", "]]") + "]"

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

        s = self._qi(self.schema_name)
        q = f"""
            SELECT CAST(SUM(CASE WHEN f.v IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT)
                       / NULLIF(COUNT(*), 0) AS overlap
            FROM (
                SELECT DISTINCT TOP (%s) {self._qi(local_column)} AS v
                FROM {s}.{self._qi(local_table)}
                WHERE {self._qi(local_column)} IS NOT NULL
            ) l
            LEFT JOIN (
                SELECT DISTINCT TOP (%s) {self._qi(foreign_column)} AS v
                FROM {s}.{self._qi(foreign_table)}
            ) f ON l.v = f.v
        """  # noqa: S608 - identifiers are bracket-quoted; schema is from the catalog
        try:
            cur = conn.cursor()
            try:
                cur.execute(q, (self.limit, self.limit))
                row = cur.fetchone()
            finally:
                cur.close()
            if not row or row[0] is None:
                return None
            return float(row[0])
        except Exception as err:  # noqa: BLE001
            logger.warning(
                "fk_sampler_query_failed",
                local=f"{local_table}.{local_column}",
                foreign=f"{foreign_table}.{foreign_column}",
                error=str(err),
            )
            return None

    # ── Denormalization probes (PRD Phase 11) ──────────────────────

    def _scalar(self, query: str, params: tuple) -> SamplerResult:
        try:
            conn = self._conn_lazy()
        except Exception as err:  # noqa: BLE001
            logger.warning("denorm_sampler_connect_failed", error=str(err))
            return None
        try:
            cur = conn.cursor()
            try:
                cur.execute(query, params)
                row = cur.fetchone()
            finally:
                cur.close()
            if not row or row[0] is None:
                return None
            return float(row[0])
        except Exception as err:  # noqa: BLE001
            logger.warning("denorm_sampler_query_failed", error=str(err))
            return None

    def distinct_ratio(self, table: str, column: str) -> SamplerResult:
        s = self._qi(self.schema_name)
        q = f"""
            SELECT CAST(COUNT(DISTINCT v) AS FLOAT) / NULLIF(COUNT(*), 0) AS ratio
            FROM (
                SELECT TOP (%s) {self._qi(column)} AS v
                FROM {s}.{self._qi(table)}
                WHERE {self._qi(column)} IS NOT NULL
            ) l
        """  # noqa: S608 - identifiers are bracket-quoted; schema is from the catalog
        return self._scalar(q, (self.limit,))

    def group_single_valued(
        self, table: str, determinant_columns: list[str], dependent_column: str
    ) -> SamplerResult:
        s = self._qi(self.schema_name)
        det = ", ".join(self._qi(c) for c in determinant_columns)
        not_null = " AND ".join(f"{self._qi(c)} IS NOT NULL" for c in determinant_columns)
        q = f"""
            SELECT AVG(CAST(CASE WHEN dcount <= 1 THEN 1.0 ELSE 0.0 END AS FLOAT)) AS frac
            FROM (
                SELECT COUNT(DISTINCT dep) AS dcount
                FROM (
                    SELECT TOP (%s) {det}, {self._qi(dependent_column)} AS dep
                    FROM {s}.{self._qi(table)}
                ) l
                WHERE {not_null}
                GROUP BY {det}
            ) g
        """  # noqa: S608 - identifiers are bracket-quoted; schema is from the catalog
        return self._scalar(q, (self.limit,))

    def delimiter_rate(self, table: str, column: str, delimiter: str) -> SamplerResult:
        s = self._qi(self.schema_name)
        q = f"""
            SELECT AVG(CAST(CASE WHEN CHARINDEX(%s, v) > 0 THEN 1.0 ELSE 0.0 END AS FLOAT)) AS rate
            FROM (
                SELECT TOP (%s) {self._qi(column)} AS v
                FROM {s}.{self._qi(table)}
                WHERE {self._qi(column)} IS NOT NULL
            ) l
        """  # noqa: S608 - identifiers are bracket-quoted; schema is from the catalog
        return self._scalar(q, (delimiter, self.limit))

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

    # ── Denormalization probes (PRD Phase 11) ──────────────────────

    def _read_columns(self, table: str, columns: list[str]):
        """Read ``columns`` of ``table`` as raw text (type inference off), bounded
        by ``limit``; returns a Polars frame or ``None`` if unreadable."""
        import polars as pl

        path = self._resolve(table)
        if path is None:
            return None
        try:
            return pl.read_csv(
                str(path),
                separator=self.delimiter,
                has_header=self.has_header,
                columns=columns,
                n_rows=self.limit,
                infer_schema_length=0,
            )
        except Exception as err:  # noqa: BLE001
            logger.warning(
                "csv_denorm_sampler_read_failed", table=table, columns=columns, error=str(err)
            )
            return None

    def distinct_ratio(self, table: str, column: str) -> SamplerResult:
        import polars as pl

        frame = self._read_columns(table, [column])
        if frame is None or not frame.columns:
            return None
        series = frame.get_column(frame.columns[0]).filter(
            pl.col(frame.columns[0]).is_not_null() & (pl.col(frame.columns[0]) != "")
        )
        total = series.len()
        if total == 0:
            return None
        return float(series.n_unique() / total)

    def group_single_valued(
        self, table: str, determinant_columns: list[str], dependent_column: str
    ) -> SamplerResult:
        import polars as pl

        frame = self._read_columns(table, [*determinant_columns, dependent_column])
        if frame is None or not frame.columns:
            return None
        for d in determinant_columns:
            frame = frame.filter(pl.col(d).is_not_null() & (pl.col(d) != ""))
        if frame.is_empty():
            return None
        grouped = frame.group_by(determinant_columns).agg(
            pl.col(dependent_column).n_unique().alias("dcount")
        )
        if grouped.is_empty():
            return None
        single = grouped.get_column("dcount") <= 1
        return float(single.sum() / single.len())

    def delimiter_rate(self, table: str, column: str, delimiter: str) -> SamplerResult:
        import polars as pl

        frame = self._read_columns(table, [column])
        if frame is None or not frame.columns:
            return None
        name = frame.columns[0]
        series = frame.get_column(name).filter(
            pl.col(name).is_not_null() & (pl.col(name) != "")
        )
        total = series.len()
        if total == 0:
            return None
        contains = series.str.contains(delimiter, literal=True)
        return float(contains.sum() / total)

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
