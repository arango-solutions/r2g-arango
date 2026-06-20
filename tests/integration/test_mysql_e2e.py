"""End-to-end integration tests for the MySQL / MariaDB source connector.

Run against live MySQL + ArangoDB Docker instances (see docker-compose.yml).
Skipped automatically when either service is unreachable, so they are safe to
collect in any environment.

These exercise the paths the unit tests can only mock: real
``information_schema`` introspection and the live ``MySQLSession`` reads
(consistent-snapshot transaction, server-side cursor streaming, CSV dump).
"""

from __future__ import annotations

import csv

from r2g.config import ConfigManager, validate_config
from r2g.connectors.arango_writer import ArangoWriter
from r2g.connectors.mysql import MySQLConnector
from r2g.streaming.pipeline import StreamingPipeline

from .conftest import (
    ARANGO_ENDPOINT,
    ARANGO_PASSWORD,
    ARANGO_USER,
    requires_mysql_arango,
)


@requires_mysql_arango
class TestMySQLEndToEnd:
    def test_introspection_captures_tables_pks_and_fks(self, mysql_conn_string):
        schema = MySQLConnector(mysql_conn_string).get_schema()

        assert set(schema.tables) == {"customers", "products", "orders", "order_items"}

        # single-column PK
        assert schema.tables["customers"].primary_key == ["customer_id"]
        # composite PK, order preserved
        assert schema.tables["order_items"].primary_key == ["order_id", "product_id"]

        # single FK: orders -> customers
        orders_fks = schema.tables["orders"].foreign_keys
        assert any(
            fk.columns == ["customer_id"] and fk.foreign_table == "customers"
            for fk in orders_fks
        )

        # join table: two FKs out of order_items
        oi_fks = schema.tables["order_items"].foreign_keys
        targets = {fk.foreign_table for fk in oi_fks}
        assert targets == {"orders", "products"}

        # a MySQL-specific type round-trips through the type map
        is_active = next(
            c for c in schema.tables["customers"].columns if c.name == "is_active"
        )
        assert is_active.data_type == "tinyint"

    def test_stream_and_verify(self, mysql_conn_string, arango_test_db):
        db_name, db = arango_test_db

        connector = MySQLConnector(mysql_conn_string)
        schema = connector.get_schema()
        config = ConfigManager.generate_default_config(schema)
        assert validate_config(schema, config) == []

        writer = ArangoWriter(
            endpoint=ARANGO_ENDPOINT,
            database=db_name,
            username=ARANGO_USER,
            password=ARANGO_PASSWORD,
        )
        pipeline = StreamingPipeline(
            source_connector=connector,
            arango_writer=writer,
            schema=schema,
            config=config,
            batch_size=100,
        )

        results = pipeline.run(graph_name="mysql_inttest_graph")

        total_docs = sum(c for _, c in results["documents"])
        total_edges = sum(c for _, c in results["edges"])
        assert total_docs > 0
        assert total_edges > 0

        assert db.has_graph("mysql_inttest_graph")
        for name, count in results["documents"]:
            assert db.collection(name).count() == count
        for name, count in results["edges"]:
            assert db.collection(name).count() == count

    def test_dry_run_writes_nothing(self, mysql_conn_string, arango_test_db):
        db_name, db = arango_test_db
        connector = MySQLConnector(mysql_conn_string)
        schema = connector.get_schema()
        config = ConfigManager.generate_default_config(schema)

        writer = ArangoWriter(
            endpoint=ARANGO_ENDPOINT,
            database=db_name,
            username=ARANGO_USER,
            password=ARANGO_PASSWORD,
        )
        pipeline = StreamingPipeline(
            source_connector=connector,
            arango_writer=writer,
            schema=schema,
            config=config,
            dry_run=True,
        )
        results = pipeline.run(graph_name="mysql_should_not_exist")

        assert sum(c for _, c in results["documents"]) > 0
        assert len(pipeline.previews) > 0
        assert not db.has_graph("mysql_should_not_exist")
        user_colls = [c["name"] for c in db.collections() if not c["name"].startswith("_")]
        assert user_colls == []

    def test_session_count_stream_and_dump(self, mysql_conn_string, tmp_path):
        connector = MySQLConnector(mysql_conn_string)
        with connector.open_session() as session:
            # count_rows
            assert session.count_rows("customers") == 3

            # stream_rows (server-side cursor) yields dict rows
            rows = list(session.stream_rows("customers", batch_size=2))
            assert len(rows) == 3
            assert {r["name"] for r in rows} == {"Alice", "Bob", "Carol"}

            # dump_table_to_csv writes header + one line per row
            out = tmp_path / "products.csv"
            written = session.dump_table_to_csv("products", out)
            assert written == 3
            with out.open(newline="") as f:
                reader = csv.reader(f)
                header = next(reader)
                data = list(reader)
            assert header == ["product_id", "title", "price"]
            assert len(data) == 3
