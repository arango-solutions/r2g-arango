from __future__ import annotations

from collections import OrderedDict

import psycopg
from psycopg.rows import dict_row

from r2g.types import Column, ForeignKey, Schema, Table


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
