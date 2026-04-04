"""End-to-end integration tests for the streaming pipeline.

These tests run against live PostgreSQL and ArangoDB Docker instances.
Skipped automatically when either service is unreachable.
"""

from __future__ import annotations

from r2g.config import ConfigManager, validate_config
from r2g.connectors.arango_writer import ArangoWriter
from r2g.connectors.postgres import PostgresConnector
from r2g.streaming.pipeline import StreamingPipeline

from .conftest import (
    ARANGO_ENDPOINT,
    ARANGO_PASSWORD,
    ARANGO_USER,
    requires_both,
)


@requires_both
class TestStreamEndToEnd:
    def test_introspect_stream_and_verify(self, pg_conn_string, arango_test_db):
        db_name, db = arango_test_db

        connector = PostgresConnector(pg_conn_string)
        schema = connector.get_schema()
        assert len(schema.tables) >= 5

        config = ConfigManager.generate_default_config(schema)
        issues = validate_config(schema, config)
        assert issues == [], f"Config validation failed: {issues}"

        writer = ArangoWriter(
            endpoint=ARANGO_ENDPOINT,
            database=db_name,
            username=ARANGO_USER,
            password=ARANGO_PASSWORD,
        )

        pipeline = StreamingPipeline(
            pg_conn_string=pg_conn_string,
            arango_writer=writer,
            schema=schema,
            config=config,
            batch_size=100,
        )

        results = pipeline.run(graph_name="inttest_graph")

        total_docs = sum(c for _, c in results["documents"])
        total_edges = sum(c for _, c in results["edges"])
        assert total_docs > 0
        assert total_edges > 0

        assert db.has_graph("inttest_graph")
        for name, count in results["documents"]:
            coll = db.collection(name)
            assert coll.count() == count

        for name, count in results["edges"]:
            coll = db.collection(name)
            assert coll.count() == count

    def test_dry_run_writes_nothing(self, pg_conn_string, arango_test_db):
        db_name, db = arango_test_db

        connector = PostgresConnector(pg_conn_string)
        schema = connector.get_schema()
        config = ConfigManager.generate_default_config(schema)

        writer = ArangoWriter(
            endpoint=ARANGO_ENDPOINT,
            database=db_name,
            username=ARANGO_USER,
            password=ARANGO_PASSWORD,
        )

        pipeline = StreamingPipeline(
            pg_conn_string=pg_conn_string,
            arango_writer=writer,
            schema=schema,
            config=config,
            dry_run=True,
        )

        results = pipeline.run(graph_name="should_not_exist")

        total_docs = sum(c for _, c in results["documents"])
        assert total_docs > 0
        assert len(pipeline.previews) > 0

        assert not db.has_graph("should_not_exist")
        collections = [c["name"] for c in db.collections() if not c["name"].startswith("_")]
        assert len(collections) == 0

    def test_drop_collections_clears_data(self, pg_conn_string, arango_test_db):
        db_name, db = arango_test_db

        connector = PostgresConnector(pg_conn_string)
        schema = connector.get_schema()
        config = ConfigManager.generate_default_config(schema)

        writer = ArangoWriter(
            endpoint=ARANGO_ENDPOINT,
            database=db_name,
            username=ARANGO_USER,
            password=ARANGO_PASSWORD,
        )

        pipeline1 = StreamingPipeline(
            pg_conn_string=pg_conn_string,
            arango_writer=writer,
            schema=schema,
            config=config,
        )
        r1 = pipeline1.run()
        first_total = sum(c for _, c in r1["documents"])

        pipeline2 = StreamingPipeline(
            pg_conn_string=pg_conn_string,
            arango_writer=writer,
            schema=schema,
            config=config,
            drop_collections=True,
        )
        r2 = pipeline2.run()
        second_total = sum(c for _, c in r2["documents"])

        assert first_total == second_total
        for name, count in r2["documents"]:
            assert db.collection(name).count() == count

    def test_progress_callback_fires(self, pg_conn_string, arango_test_db):
        db_name, _ = arango_test_db

        connector = PostgresConnector(pg_conn_string)
        schema = connector.get_schema()
        config = ConfigManager.generate_default_config(schema)

        writer = ArangoWriter(
            endpoint=ARANGO_ENDPOINT,
            database=db_name,
            username=ARANGO_USER,
            password=ARANGO_PASSWORD,
        )

        pipeline = StreamingPipeline(
            pg_conn_string=pg_conn_string,
            arango_writer=writer,
            schema=schema,
            config=config,
        )

        events: list[tuple[str, str, int, int | None]] = []

        def on_progress(event: str, name: str, current: int, total: int | None) -> None:
            events.append((event, name, current, total))

        pipeline.run(on_progress=on_progress)

        start_events = [e for e in events if e[0] == "start"]
        done_events = [e for e in events if e[0] == "done"]
        assert len(start_events) > 0
        assert len(done_events) == len(start_events)
        for _, name, count, total in done_events:
            assert count >= 0
            assert total is not None

    def test_parallel_streaming(self, pg_conn_string, arango_test_db):
        """Parallel mode (workers=4) produces correct data."""
        db_name, db = arango_test_db
        connector = PostgresConnector(pg_conn_string)
        schema = connector.get_schema()
        config = ConfigManager.generate_default_config(schema)
        writer = ArangoWriter(
            endpoint=ARANGO_ENDPOINT, database=db_name,
            username=ARANGO_USER, password=ARANGO_PASSWORD,
        )
        pipeline = StreamingPipeline(
            pg_conn_string=pg_conn_string, arango_writer=writer,
            schema=schema, config=config, workers=4,
        )
        results = pipeline.run(graph_name="parallel_graph")
        total_docs = sum(c for _, c in results["documents"])
        total_edges = sum(c for _, c in results["edges"])
        assert total_docs > 0
        assert total_edges > 0
        assert "elapsed_seconds" in results
        assert db.has_graph("parallel_graph")
        for name, count in results["documents"]:
            assert db.collection(name).count() == count
        for name, count in results["edges"]:
            assert db.collection(name).count() == count

    def test_timing_in_results(self, pg_conn_string, arango_test_db):
        """Pipeline results include elapsed_seconds timing."""
        db_name, _ = arango_test_db
        connector = PostgresConnector(pg_conn_string)
        schema = connector.get_schema()
        config = ConfigManager.generate_default_config(schema)
        writer = ArangoWriter(
            endpoint=ARANGO_ENDPOINT, database=db_name,
            username=ARANGO_USER, password=ARANGO_PASSWORD,
        )
        pipeline = StreamingPipeline(
            pg_conn_string=pg_conn_string, arango_writer=writer,
            schema=schema, config=config,
        )
        results = pipeline.run()
        assert "elapsed_seconds" in results
        assert isinstance(results["elapsed_seconds"], float)
        assert results["elapsed_seconds"] > 0
