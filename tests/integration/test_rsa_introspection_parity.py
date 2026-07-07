"""RSA <-> r2g introspection parity audit (integration, live DBs).

Stage 2 of the RSA dependency reversal deliberately keeps r2g's *introspection*
connectors local (see ``docs/internal/PLAN-rsa-dependency-reversal.md``): RSA's
per-dialect connectors have diverged and emit RSA-typed objects. This test is an
**audit**, not a shipped feature — it quantifies how far r2g's and RSA's live
introspection currently diverge, which is the evidence a future decision to reuse
RSA's introspectors would need.

For each case it introspects the *same* source with both r2g's connector and RSA's
connector, normalizes both into r2g's serialized ``Schema`` shape (RSA's output
re-validated into ``r2g.types.Schema``, which drops RSA's enrichment fields), and
compares:

- **Structural invariants (asserted on the shared tables):** for every table both
  connectors introspect, the per-table column names and primary keys must match. A
  future reuse is only viable if these agree.
- **Membership differences (recorded, non-fatal):** objects only one side returns.
  This is the load-bearing finding of the audit — e.g. on ``pagila`` RSA's Postgres
  introspector also returns the 7 views + 1 materialized view that r2g omits (r2g
  introspects base tables only). Reusing RSA's introspector would therefore start
  surfacing views as loadable collections unless filtered.
- **Cosmetic/behavioural differences (recorded, non-fatal):** per-column
  ``data_type`` / ``is_nullable`` / ``is_primary_key`` and declared
  ``foreign_keys`` on the shared tables.

All differences are printed and attached via ``record_property`` so a run captures
the divergence report without failing the suite on membership-only differences.

The corpus intentionally spans edge cases so the audit is meaningful:

- **PostgreSQL** ``northwind`` (classic), ``chinook`` (larger, integer PKs), and
  ``pagila`` (ENUM ``mpaa_rating``, ``DOMAIN`` types, ``text[]`` arrays,
  ``tsvector``, composite PKs like ``film_actor``/``film_category``, and a
  partitioned ``payment`` table).
- **MySQL** / **SQL Server** ``shop`` (declared FKs across dialects).
- **CSV** ``docker/csv_demo`` (header-inferred columns, no PK/FK).

Skipped per-case when the source is unreachable.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

# RSA exposes the same factory name via a back-compat alias.
from relational_schema_analyzer.connectors.base import (
    create_source_connector as rsa_create_source_connector,
)

from r2g.connectors.base import create_source_connector
from r2g.types import Schema

from .conftest import (
    MYSQL_CONN,
    PG_CONN,
    _mssql_available,
    _mysql_available,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]

# PostgreSQL corpus: the docker-compose stack seeds all three into one instance
# (northwind as POSTGRES_DB, chinook + pagila via docker/load-samples.sh).
PG_CORPUS = ["northwind", "chinook", "pagila"]

requires_mysql_only = pytest.mark.skipif(not _mysql_available(), reason="MySQL not available")
requires_mssql_only = pytest.mark.skipif(not _mssql_available(), reason="SQL Server not available")


def _pg_dsn(db: str) -> str:
    """Return PG_CONN with its database (last path segment) swapped for *db*."""
    base, _, _ = PG_CONN.rpartition("/")
    return f"{base}/{db}"


def _pg_db_available(db: str) -> bool:
    try:
        import psycopg

        with psycopg.connect(_pg_dsn(db)) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def _r2g_shape(schema: Any) -> dict[str, Any]:
    """Normalize any schema (r2g ``Schema`` or RSA ``PhysicalSchema``) into r2g's
    serialized shape so the two are directly comparable."""
    return Schema.model_validate(schema.model_dump()).model_dump()


def _compare(
    case: str,
    r2g_dump: dict[str, Any],
    rsa_dump: dict[str, Any],
    record_property,
) -> None:
    r2g_tables = r2g_dump["tables"]
    rsa_tables = rsa_dump["tables"]

    ncols = sum(len(t["columns"]) for t in r2g_tables.values())
    nfks = sum(len(t["foreign_keys"]) for t in r2g_tables.values())

    # --- Membership differences (recorded, non-fatal) ---
    r2g_only = sorted(set(r2g_tables) - set(rsa_tables))
    rsa_only = sorted(set(rsa_tables) - set(r2g_tables))
    shared = sorted(set(r2g_tables) & set(rsa_tables))

    diffs: list[str] = []
    if r2g_only:
        diffs.append(f"objects only r2g returns ({len(r2g_only)}): {r2g_only}")
    if rsa_only:
        diffs.append(f"objects only RSA returns ({len(rsa_only)}): {rsa_only}")

    # --- Structural invariants (asserted on shared tables) ---
    for tname in shared:
        r_cols = [c["name"] for c in r2g_tables[tname]["columns"]]
        s_cols = [c["name"] for c in rsa_tables[tname]["columns"]]
        assert r_cols == s_cols, f"[{case}] {tname}: column names/order differ"
        assert r2g_tables[tname]["primary_key"] == rsa_tables[tname]["primary_key"], (
            f"[{case}] {tname}: primary key differs "
            f"(r2g={r2g_tables[tname]['primary_key']} rsa={rsa_tables[tname]['primary_key']})"
        )

    # --- Cosmetic / behavioural differences on shared tables (recorded, non-fatal) ---
    for tname in shared:
        r_by = {c["name"]: c for c in r2g_tables[tname]["columns"]}
        s_by = {c["name"]: c for c in rsa_tables[tname]["columns"]}
        for cname, rcol in r_by.items():
            scol = s_by[cname]
            for field in ("data_type", "is_nullable", "is_primary_key"):
                if rcol.get(field) != scol.get(field):
                    diffs.append(
                        f"{tname}.{cname}.{field}: r2g={rcol.get(field)!r} rsa={scol.get(field)!r}"
                    )
        if r2g_tables[tname]["foreign_keys"] != rsa_tables[tname]["foreign_keys"]:
            diffs.append(
                f"{tname}.foreign_keys differ: "
                f"r2g={r2g_tables[tname]['foreign_keys']} rsa={rsa_tables[tname]['foreign_keys']}"
            )

    summary = (
        f"[{case}] shared tables={len(shared)} (r2g={len(r2g_tables)} rsa={len(rsa_tables)}) "
        f"columns={ncols} fks={nfks}; {len(diffs)} recorded diff(s)"
    )
    record_property("parity_summary", summary)
    record_property("parity_diffs", "\n".join(diffs))
    print("\n" + summary)
    for d in diffs:
        print("  - " + d)


@pytest.mark.parametrize("pg_db", PG_CORPUS)
def test_pg_introspection_parity(pg_db, record_property):
    if not _pg_db_available(pg_db):
        pytest.skip(f"PostgreSQL database {pg_db!r} not available")
    dsn = _pg_dsn(pg_db)
    r2g_schema = create_source_connector("postgresql", dsn).get_schema()
    rsa_schema = rsa_create_source_connector("postgresql", dsn).get_schema()
    _compare(f"postgresql/{pg_db}", _r2g_shape(r2g_schema), _r2g_shape(rsa_schema), record_property)


@requires_mysql_only
def test_mysql_introspection_parity(record_property):
    r2g_schema = create_source_connector("mysql", MYSQL_CONN).get_schema()
    rsa_schema = rsa_create_source_connector("mysql", MYSQL_CONN).get_schema()
    _compare("mysql/shop", _r2g_shape(r2g_schema), _r2g_shape(rsa_schema), record_property)


@requires_mssql_only
def test_mssql_introspection_parity(sqlserver_conn_string, record_property):
    # The fixture creates the target DB and loads docker/mssql_demo/schema.sql
    # (SQL Server has no init-script hook), then yields the DSN.
    dsn = sqlserver_conn_string
    r2g_schema = create_source_connector("sqlserver", dsn).get_schema()
    rsa_schema = rsa_create_source_connector("sqlserver", dsn).get_schema()
    _compare("sqlserver/shop", _r2g_shape(r2g_schema), _r2g_shape(rsa_schema), record_property)


def test_csv_introspection_parity(record_property):
    csv_dir = _REPO_ROOT / "docker" / "csv_demo"
    if not csv_dir.is_dir() or not list(csv_dir.glob("*.csv")):
        pytest.skip(f"CSV demo directory not available at {csv_dir}")
    params = {"delimiter": ",", "has_header": True}
    try:
        r2g_schema = create_source_connector(
            "csv", str(csv_dir), source_params=params
        ).get_schema()
        rsa_schema = rsa_create_source_connector(
            "csv", str(csv_dir), source_params=params
        ).get_schema()
    except ImportError as exc:  # optional CSV dependency not installed
        pytest.skip(f"CSV connector dependency unavailable: {exc}")
    _compare("csv/csv_demo", _r2g_shape(r2g_schema), _r2g_shape(rsa_schema), record_property)
