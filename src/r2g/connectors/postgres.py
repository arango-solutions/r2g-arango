import psycopg
from psycopg.rows import dict_row
from typing import Dict, List, Any
from r2g.types import Schema, Table, Column, ForeignKey

class PostgresConnector:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        
    def get_schema(self) -> Schema:
        """
        Connects to PostgreSQL and inspects the schema.
        Returns a Schema object populated with tables, columns, PKs, and FKs.
        """
        schema = Schema()
        
        try:
            with psycopg.connect(self.connection_string, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    # 1. Get Tables (public schema only for MVP)
                    cur.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                          AND table_type = 'BASE TABLE';
                    """)
                    tables = cur.fetchall()
                    
                    for t in tables:
                        table_name = t['table_name']
                        schema.tables[table_name] = self._process_table(cur, table_name)
                        
        except Exception as e:
            # Re-raise or handle error appropriately
            raise RuntimeError(f"Failed to fetch schema from PostgreSQL: {e}")
            
        return schema

    def _process_table(self, cur, table_name: str) -> Table:
        # 2. Get Columns
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        columns_data = cur.fetchall()
        
        # 3. Get Primary Keys
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = 'public'
              AND tc.table_name = %s;
        """, (table_name,))
        pks = [row['column_name'] for row in cur.fetchall()]
        
        # Build Column objects
        columns = []
        for c in columns_data:
            col = Column(
                name=c['column_name'],
                data_type=c['data_type'],
                is_nullable=(c['is_nullable'] == 'YES'),
                is_primary_key=(c['column_name'] in pks)
            )
            columns.append(col)
            
        # 4. Get Foreign Keys
        cur.execute("""
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                tc.constraint_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
              AND tc.table_name = %s;
        """, (table_name,))
        fks_data = cur.fetchall()
        
        fks = []
        for fk in fks_data:
            fks.append(ForeignKey(
                column=fk['column_name'],
                foreign_table=fk['foreign_table_name'],
                foreign_column=fk['foreign_column_name'],
                constraint_name=fk['constraint_name']
            ))
            
        return Table(
            name=table_name,
            columns=columns,
            primary_key=pks,
            foreign_keys=fks
        )
