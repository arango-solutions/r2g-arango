"""PostgreSQL source connector.

Provides:

- :class:`PostgresConnector` — schema introspection (the original scope).
- :class:`PostgresSession` — the Phase 6 slice 3 bulk-read session
  (consistent-snapshot transaction, server-side cursor streaming,
  ``COPY TO STDOUT`` CSV export). Both the streaming pipeline and the
  ``source dump`` CLI now consume PG through this interface.

PG has two fast paths we care about:

1. ``SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`` — gives a
   point-in-time snapshot for the session's lifetime across every
   table read, matching what ``StreamingPipeline`` has required since
   day one.
2. ``COPY <table> TO STDOUT WITH CSV HEADER`` — 10-100x faster than
   row-by-row fetches for export. We route ``dump_table_to_csv``
   through it.
"""

from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterator, Optional

import psycopg
from psycopg.rows import dict_row, tuple_row

from r2g.types import Column, ForeignKey, Schema, Table


def preview_table_rows(
    connection_string: str,
    schema_name: str,
    table_name: str,
    limit: int,
) -> list[dict]:
    """Return up to ``limit`` rows from ``schema_name.table_name`` as JSON-safe dicts.

    Both identifiers are bound through psycopg's ``sql.Identifier`` so they are
    never string-interpolated into SQL. Callers MUST still validate
    ``table_name`` against a trusted schema snapshot first — this guards against
    quoting bugs, not against previewing an arbitrary table. Shared by the UI
    and MCP preview endpoints.
    """
    from psycopg import sql

    from r2g.connectors.base import serialize_rows

    query = sql.SQL("SELECT * FROM {}.{} LIMIT %s").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
    )
    with psycopg.connect(connection_string, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()
    return serialize_rows(rows)


class PostgresConnector:
    def __init__(self, connection_string: str, schema_name: str = "public") -> None:
        self.connection_string = connection_string
        self.schema_name = schema_name

    def get_schema(self) -> Schema:
        """Connect to PostgreSQL and inspect the schema.

        Returns a Schema object populated with tables, columns, PKs, and FKs.
        """
        schema = Schema()

        try:
            with psycopg.connect(self.connection_string, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = %s
                          AND table_type = 'BASE TABLE';
                        """,
                        (self.schema_name,),
                    )
                    tables = cur.fetchall()

                    for t in tables:
                        table_name = t["table_name"]
                        schema.tables[table_name] = self._process_table(cur, table_name)

        except Exception as e:
            raise RuntimeError(f"Failed to fetch schema from PostgreSQL: {e}")

        return schema

    def open_session(self) -> "PostgresSession":
        """Open a REPEATABLE READ read-only session for streaming / dumps."""
        return PostgresSession(self.connection_string, schema_name=self.schema_name)

    def _process_table(self, cur: psycopg.Cursor, table_name: str) -> Table:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (self.schema_name, table_name),
        )
        columns_data = cur.fetchall()

        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s;
            """,
            (self.schema_name, table_name),
        )
        pks = [row["column_name"] for row in cur.fetchall()]

        columns = []
        for c in columns_data:
            columns.append(
                Column(
                    name=c["column_name"],
                    data_type=c["data_type"],
                    is_nullable=(c["is_nullable"] == "YES"),
                    is_primary_key=(c["column_name"] in pks),
                )
            )

        # pg_catalog gives correct positional pairing for composite FKs
        cur.execute(
            """
            SELECT
                a.attname  AS column_name,
                cf.relname AS foreign_table_name,
                af.attname AS foreign_column_name,
                c.conname  AS constraint_name
            FROM pg_constraint c
            JOIN pg_class cr ON c.conrelid = cr.oid
            JOIN pg_namespace nr ON cr.relnamespace = nr.oid
            JOIN pg_class cf ON c.confrelid = cf.oid
            CROSS JOIN LATERAL unnest(c.conkey, c.confkey)
                WITH ORDINALITY AS u(local_col, ref_col, ord)
            JOIN pg_attribute a  ON a.attrelid = c.conrelid  AND a.attnum = u.local_col
            JOIN pg_attribute af ON af.attrelid = c.confrelid AND af.attnum = u.ref_col
            WHERE c.contype = 'f'
              AND nr.nspname = %s
              AND cr.relname = %s
            ORDER BY c.conname, u.ord;
            """,
            (self.schema_name, table_name),
        )
        fks_data = cur.fetchall()

        grouped: OrderedDict[str, dict] = OrderedDict()
        for fk in fks_data:
            cname = fk["constraint_name"]
            if cname not in grouped:
                grouped[cname] = {
                    "columns": [],
                    "foreign_table": fk["foreign_table_name"],
                    "foreign_columns": [],
                    "constraint_name": cname,
                }
            grouped[cname]["columns"].append(fk["column_name"])
            grouped[cname]["foreign_columns"].append(fk["foreign_column_name"])

        fks = [ForeignKey(**v) for v in grouped.values()]

        return Table(
            name=table_name,
            columns=columns,
            primary_key=pks,
            foreign_keys=fks,
        )


class PostgresSession:
    """Bulk-read session for :class:`PostgresConnector`.

    Holds a single autocommit=False connection with ``REPEATABLE READ``
    isolation for consistent snapshots across every table read during
    a pipeline pass. Each instance owns its connection; call
    :meth:`close` when done.
    """

    def __init__(self, connection_string: str, *, schema_name: str = "public") -> None:
        self.connection_string = connection_string
        self.schema_name = schema_name
        self._conn: Optional[psycopg.Connection] = None

    @property
    def connection(self) -> psycopg.Connection:
        if self._conn is None:
            self._conn = psycopg.connect(
                self.connection_string,
                row_factory=dict_row,
                autocommit=False,
            )
            self._conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def __enter__(self) -> "PostgresSession":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def count_rows(
        self,
        table: str,
        *,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> int:
        qualified = f'"{self.schema_name}"."{table}"'
        conn = self.connection
        with conn.cursor(row_factory=tuple_row) as cur:
            if since_column and since_value is not None:
                cur.execute(
                    f'SELECT count(*) FROM {qualified} '  # noqa: S608
                    f'WHERE "{since_column}" >= %s',
                    (since_value,),
                )
            else:
                cur.execute(f"SELECT count(*) FROM {qualified}")  # noqa: S608
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def stream_rows(
        self,
        table: str,
        *,
        batch_size: int = 10_000,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]:
        qualified = f'"{self.schema_name}"."{table}"'
        cursor_name = f"r2g_{table}"
        conn = self.connection
        with conn.cursor(name=cursor_name, row_factory=dict_row) as cur:
            cur.itersize = max(1, batch_size)
            if since_column and since_value is not None:
                cur.execute(
                    f'SELECT * FROM {qualified} '  # noqa: S608
                    f'WHERE "{since_column}" >= %s',
                    (since_value,),
                )
            else:
                cur.execute(f"SELECT * FROM {qualified}")  # noqa: S608
            yield from cur

    def dump_table_to_csv(
        self,
        table: str,
        out_path: Path,
        *,
        header: bool = True,
    ) -> int:
        """Export *table* via ``COPY TO STDOUT WITH CSV``.

        Uses the server-side fast path rather than Python-level row
        fetching. Returns the number of data rows written (i.e.
        excluding the header).
        """
        qualified = f'"{self.schema_name}"."{table}"'
        header_clause = "CSV HEADER" if header else "CSV"
        copy_sql = f"COPY {qualified} TO STDOUT WITH {header_clause}"  # noqa: S608
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self.connection
        with out_path.open("wb") as f:
            with conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:
                    for chunk in copy:
                        f.write(chunk)
        with out_path.open("rb") as f:
            total = sum(1 for _ in f)
        return max(0, total - (1 if header else 0))


__all__ = ["PostgresConnector", "PostgresSession"]
