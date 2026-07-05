"""RSA <-> r2g introspection parity audit (integration, live DBs).

Stage 2 of the RSA dependency reversal deliberately keeps r2g's *introspection*
connectors local (see ``docs/internal/PLAN-rsa-dependency-reversal.md``): RSA's
per-dialect connectors have diverged and emit RSA-typed objects. This test is an
**audit**, not a shipped feature — it quantifies how far r2g's and RSA's live
introspection currently diverge, which is the evidence a future decision to reuse
RSA's introspectors would need.

For each SQL dialect it introspects the *same* database with both r2g's connector
and RSA's connector, normalizes both into r2g's serialized ``Schema`` shape (RSA's
output re-validated into ``r2g.types.Schema``, which drops RSA's enrichment
fields), and compares:

- **Structural invariants (asserted):** identical table names, per-table column
  names, and primary keys. A future reuse is only viable if these match.
- **Cosmetic/behavioural differences (recorded, non-fatal):** per-column
  ``data_type`` / ``is_nullable`` / ``is_primary_key`` and declared
  ``foreign_keys``. These are printed and attached via ``record_property`` so a
  test run captures the divergence report without failing the suite.

Skipped automatically when the databases are unreachable.
"""

from __future__ import annotations

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
    _pg_available,
)

requires_pg_only = pytest.mark.skipif(not _pg_available(), reason="PostgreSQL not available")
requires_mysql_only = pytest.mark.skipif(not _mysql_available(), reason="MySQL not available")
requires_mssql_only = pytest.mark.skipif(not _mssql_available(), reason="SQL Server not available")


def _r2g_shape(schema: Any) -> dict[str, Any]:
    """Normalize any schema (r2g ``Schema`` or RSA ``PhysicalSchema``) into r2g's
    serialized shape so the two are directly comparable."""
    return Schema.model_validate(schema.model_dump()).model_dump()


def _compare(
    dialect: str,
    r2g_dump: dict[str, Any],
    rsa_dump: dict[str, Any],
    record_property,
) -> None:
    r2g_tables = r2g_dump["tables"]
    rsa_tables = rsa_dump["tables"]

    # --- Structural invariants (asserted) ---
    assert set(r2g_tables) == set(rsa_tables), (
        f"[{dialect}] table set differs: "
        f"r2g-only={sorted(set(r2g_tables) - set(rsa_tables))}, "
        f"rsa-only={sorted(set(rsa_tables) - set(r2g_tables))}"
    )
    for tname in sorted(r2g_tables):
        r_cols = [c["name"] for c in r2g_tables[tname]["columns"]]
        s_cols = [c["name"] for c in rsa_tables[tname]["columns"]]
        assert r_cols == s_cols, f"[{dialect}] {tname}: column names/order differ"
        assert r2g_tables[tname]["primary_key"] == rsa_tables[tname]["primary_key"], (
            f"[{dialect}] {tname}: primary key differs "
            f"(r2g={r2g_tables[tname]['primary_key']} rsa={rsa_tables[tname]['primary_key']})"
        )

    # --- Cosmetic / behavioural differences (recorded, non-fatal) ---
    diffs: list[str] = []
    for tname in sorted(r2g_tables):
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
        f"[{dialect}] structural parity OK; {len(diffs)} cosmetic/behavioural diff(s)"
    )
    record_property("parity_summary", summary)
    record_property("parity_diffs", "\n".join(diffs))
    print("\n" + summary)
    for d in diffs:
        print("  - " + d)


@requires_pg_only
def test_pg_introspection_parity(record_property):
    r2g_schema = create_source_connector("postgresql", PG_CONN).get_schema()
    rsa_schema = rsa_create_source_connector("postgresql", PG_CONN).get_schema()
    _compare("postgresql", _r2g_shape(r2g_schema), _r2g_shape(rsa_schema), record_property)


@requires_mysql_only
def test_mysql_introspection_parity(record_property):
    r2g_schema = create_source_connector("mysql", MYSQL_CONN).get_schema()
    rsa_schema = rsa_create_source_connector("mysql", MYSQL_CONN).get_schema()
    _compare("mysql", _r2g_shape(r2g_schema), _r2g_shape(rsa_schema), record_property)


@requires_mssql_only
def test_mssql_introspection_parity(sqlserver_conn_string, record_property):
    # The fixture creates the target DB and loads docker/mssql_demo/schema.sql
    # (SQL Server has no init-script hook), then yields the DSN.
    dsn = sqlserver_conn_string
    r2g_schema = create_source_connector("sqlserver", dsn).get_schema()
    rsa_schema = rsa_create_source_connector("sqlserver", dsn).get_schema()
    _compare("sqlserver", _r2g_shape(r2g_schema), _r2g_shape(rsa_schema), record_property)
