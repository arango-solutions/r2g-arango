"""End-to-end integration tests for the SQL Server source connector.

Run against live SQL Server + ArangoDB Docker instances (see docker-compose.yml).
Skipped automatically when either service is unreachable. The schema is seeded
by the ``sqlserver_conn_string`` fixture (the SQL Server image has no
init-script hook).

These exercise the paths the unit tests can only mock: real
``INFORMATION_SCHEMA`` / ``sys.*`` introspection and the live
``SQLServerSession`` reads (batched cursor streaming + CSV dump).
"""

from __future__ import annotations

import csv

from r2g.config import ConfigManager, validate_config
from r2g.connectors.arango_writer import ArangoWriter
from r2g.connectors.mssql import SQLServerConnector
from r2g.streaming.pipeline import StreamingPipeline

from .conftest import (
    ARANGO_ENDPOINT,
    ARANGO_PASSWORD,
    ARANGO_USER,
    requires_mssql_arango,
)


@requires_mssql_arango
class TestSQLServerEndToEnd:
    def test_introspection_captures_tables_pks_fks_and_bit(self, sqlserver_conn_string):
        schema = SQLServerConnector(sqlserver_conn_string).get_schema()

        assert set(schema.tables) == {"customers", "products", "orders", "order_items"}
        assert schema.tables["customers"].primary_key == ["customer_id"]
        assert schema.tables["order_items"].primary_key == ["order_id", "product_id"]

        orders_fks = schema.tables["orders"].foreign_keys
        assert any(
            fk.columns == ["customer_id"] and fk.foreign_table == "customers"
            for fk in orders_fks
        )
        oi_targets = {fk.foreign_table for fk in schema.tables["order_items"].foreign_keys}
        assert oi_targets == {"orders", "products"}

        # BIT is translated to boolean connector-side.
        is_active = next(
            c for c in schema.tables["customers"].columns if c.name == "is_active"
        )
        assert is_active.data_type == "boolean"

    def test_stream_and_verify(self, sqlserver_conn_string, arango_test_db):
        db_name, db = arango_test_db

        connector = SQLServerConnector(sqlserver_conn_string)
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

        results = pipeline.run(graph_name="mssql_inttest_graph")

        assert sum(c for _, c in results["documents"]) > 0
        assert sum(c for _, c in results["edges"]) > 0
        assert db.has_graph("mssql_inttest_graph")
        for name, count in results["documents"]:
            assert db.collection(name).count() == count
        for name, count in results["edges"]:
            assert db.collection(name).count() == count

    def test_dry_run_writes_nothing(self, sqlserver_conn_string, arango_test_db):
        db_name, db = arango_test_db
        connector = SQLServerConnector(sqlserver_conn_string)
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
        results = pipeline.run(graph_name="mssql_should_not_exist")

        assert sum(c for _, c in results["documents"]) > 0
        assert len(pipeline.previews) > 0
        assert not db.has_graph("mssql_should_not_exist")
        user_colls = [c["name"] for c in db.collections() if not c["name"].startswith("_")]
        assert user_colls == []

    def test_session_count_stream_and_dump(self, sqlserver_conn_string, tmp_path):
        connector = SQLServerConnector(sqlserver_conn_string)
        with connector.open_session() as session:
            assert session.count_rows("customers") == 3

            rows = list(session.stream_rows("customers", batch_size=2))
            assert len(rows) == 3
            assert {r["name"] for r in rows} == {"Alice", "Bob", "Carol"}

            out = tmp_path / "products.csv"
            written = session.dump_table_to_csv("products", out)
            assert written == 3
            with out.open(newline="") as f:
                reader = csv.reader(f)
                header = next(reader)
                data = list(reader)
            assert header == ["product_id", "title", "price"]
            assert len(data) == 3
