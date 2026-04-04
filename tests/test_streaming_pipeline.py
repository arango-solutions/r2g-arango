from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from r2g.connectors.arango_writer import ArangoWriter, ImportBatchError
from r2g.streaming.pipeline import StreamingPipeline
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    ForeignKey,
    MappingConfig,
    Schema,
    Table,
)


@pytest.fixture
def simple_schema() -> Schema:
    users = Table(
        name="users",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="name", data_type="text", is_nullable=False),
            Column(name="email", data_type="text", is_nullable=True),
        ],
        primary_key=["id"],
        foreign_keys=[],
    )
    orders = Table(
        name="orders",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="user_id", data_type="integer", is_nullable=False),
            Column(name="total", data_type="numeric", is_nullable=False),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(column="user_id", foreign_table="users", foreign_column="id", constraint_name="fk_user"),
        ],
    )
    return Schema(tables={"users": users, "orders": orders})


@pytest.fixture
def simple_config() -> MappingConfig:
    return MappingConfig(
        collections={
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="orders_to_users",
                from_collection="orders",
                to_collection="users",
                from_field="user_id",
                to_field="id",
            ),
        ],
    )


@pytest.fixture
def mock_writer():
    writer = MagicMock(spec=ArangoWriter)
    writer.import_batch.return_value = {"created": 0, "errors": 0, "empty": 0, "updated": 0, "ignored": 0}
    return writer


class TestStreamingPipelineInit:
    def test_stores_params(self, simple_schema, simple_config, mock_writer):
        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            batch_size=5000,
        )
        assert pipeline.batch_size == 5000
        assert pipeline.on_duplicate == "replace"

    def test_default_batch_size(self, simple_schema, simple_config, mock_writer):
        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )
        assert pipeline.batch_size == 10_000


class TestStreamDocuments:
    @patch("r2g.streaming.pipeline.psycopg")
    def test_creates_collections_and_imports(self, mock_psycopg, simple_schema, simple_config, mock_writer):
        mock_conn = MagicMock()

        def make_cursor(*args, **kwargs):
            cursor = MagicMock()
            name = kwargs.get("name", "")
            if "users" in name:
                cursor.__iter__ = MagicMock(return_value=iter([
                    {"id": 1, "name": "Alice", "email": "a@b.com"},
                    {"id": 2, "name": "Bob", "email": None},
                ]))
            elif "orders" in name:
                cursor.__iter__ = MagicMock(return_value=iter([
                    {"id": 10, "user_id": 1, "total": 99.99},
                ]))
            else:
                cursor.__iter__ = MagicMock(return_value=iter([]))
            cursor.__enter__ = MagicMock(return_value=cursor)
            cursor.__exit__ = MagicMock(return_value=False)
            return cursor

        mock_conn.cursor.side_effect = make_cursor

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            batch_size=100,
        )

        results = pipeline._stream_documents(mock_conn)

        assert len(results) == 2
        assert mock_writer.ensure_collection.call_count == 2
        assert mock_writer.import_batch.call_count == 2


class TestStreamEdges:
    @patch("r2g.streaming.pipeline.psycopg")
    def test_creates_edge_collections_and_imports(self, mock_psycopg, simple_schema, simple_config, mock_writer):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = MagicMock(return_value=iter([
            {"id": 1, "user_id": 10, "total": 99.99},
            {"id": 2, "user_id": 20, "total": 50.0},
        ]))
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            batch_size=100,
        )

        results = pipeline._stream_edges(mock_conn)

        assert len(results) == 1
        assert results[0][0] == "orders_to_users"
        mock_writer.ensure_collection.assert_called_with("orders_to_users", edge=True)


class TestRunPipeline:
    @patch("r2g.streaming.pipeline.psycopg")
    def test_full_run_calls_connect_and_close(self, mock_psycopg, simple_schema, simple_config, mock_writer):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = MagicMock(return_value=iter([]))
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_psycopg.connect.return_value = mock_conn

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )

        results = pipeline.run()

        mock_writer.connect.assert_called_once()
        mock_writer.close.assert_called_once()
        assert "documents" in results
        assert "edges" in results

    @patch("r2g.streaming.pipeline.psycopg")
    def test_run_with_graph_creates_graph(self, mock_psycopg, simple_schema, simple_config, mock_writer):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = MagicMock(return_value=iter([]))
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_psycopg.connect.return_value = mock_conn

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )

        pipeline.run(graph_name="test_graph")

        mock_writer.create_named_graph.assert_called_once()
        args = mock_writer.create_named_graph.call_args
        assert args[0][0] == "test_graph"

    @patch("r2g.streaming.pipeline.psycopg")
    def test_sets_repeatable_read(self, mock_psycopg, simple_schema, simple_config, mock_writer):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = MagicMock(return_value=iter([]))
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_psycopg.connect.return_value = mock_conn

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )

        pipeline.run()

        mock_conn.execute.assert_any_call("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")


class TestDryRun:
    @patch("r2g.streaming.pipeline.psycopg")
    def test_dry_run_skips_arango_writes(self, mock_psycopg, simple_schema, simple_config, mock_writer):
        mock_conn = MagicMock()

        def make_cursor(*args, **kwargs):
            cursor = MagicMock()
            name = kwargs.get("name", "")
            if "users" in name:
                cursor.__iter__ = MagicMock(return_value=iter([
                    {"id": 1, "name": "Alice", "email": "a@b.com"},
                ]))
            elif "orders" in name:
                cursor.__iter__ = MagicMock(return_value=iter([
                    {"id": 10, "user_id": 1, "total": 99.99},
                ]))
            else:
                cursor.__iter__ = MagicMock(return_value=iter([]))
            cursor.__enter__ = MagicMock(return_value=cursor)
            cursor.__exit__ = MagicMock(return_value=False)
            return cursor

        mock_conn.cursor.side_effect = make_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_psycopg.connect.return_value = mock_conn

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            dry_run=True,
        )

        results = pipeline.run(graph_name="test_graph")

        mock_writer.connect.assert_called_once()
        mock_writer.close.assert_called_once()
        mock_writer.ensure_collection.assert_not_called()
        mock_writer.import_batch.assert_not_called()
        mock_writer.create_named_graph.assert_not_called()

        assert sum(c for _, c in results["documents"]) >= 1
        assert sum(c for _, c in results["edges"]) >= 1

    @patch("r2g.streaming.pipeline.psycopg")
    def test_dry_run_captures_previews(self, mock_psycopg, simple_schema, simple_config, mock_writer):
        mock_conn = MagicMock()

        def make_cursor(*args, **kwargs):
            cursor = MagicMock()
            name = kwargs.get("name", "")
            if "users" in name:
                cursor.__iter__ = MagicMock(return_value=iter([
                    {"id": 1, "name": "Alice", "email": "a@b.com"},
                    {"id": 2, "name": "Bob", "email": None},
                ]))
            elif "orders" in name:
                cursor.__iter__ = MagicMock(return_value=iter([
                    {"id": 10, "user_id": 1, "total": 99.99},
                ]))
            else:
                cursor.__iter__ = MagicMock(return_value=iter([]))
            cursor.__enter__ = MagicMock(return_value=cursor)
            cursor.__exit__ = MagicMock(return_value=False)
            return cursor

        mock_conn.cursor.side_effect = make_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_psycopg.connect.return_value = mock_conn

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            dry_run=True,
        )

        pipeline.run()

        assert "users" in pipeline.previews
        assert len(pipeline.previews["users"]) == 2
        assert pipeline.previews["users"][0]["_key"] == "1"

        assert "orders_to_users" in pipeline.previews
        assert len(pipeline.previews["orders_to_users"]) == 1


class TestTableFiltering:
    @patch("r2g.streaming.pipeline.psycopg")
    def test_include_tables_filters_documents(self, mock_psycopg, simple_schema, simple_config, mock_writer):
        mock_conn = MagicMock()

        def make_cursor(*args, **kwargs):
            cursor = MagicMock()
            name = kwargs.get("name", "")
            if "users" in name:
                cursor.__iter__ = MagicMock(return_value=iter([
                    {"id": 1, "name": "Alice", "email": "a@b.com"},
                ]))
            else:
                cursor.__iter__ = MagicMock(return_value=iter([]))
            cursor.__enter__ = MagicMock(return_value=cursor)
            cursor.__exit__ = MagicMock(return_value=False)
            return cursor

        mock_conn.cursor.side_effect = make_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_psycopg.connect.return_value = mock_conn

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            include_tables={"users"},
        )

        results = pipeline.run()

        doc_names = [name for name, _ in results["documents"]]
        assert "users" in doc_names
        assert "orders" not in doc_names
        assert len(results["edges"]) == 0

    @patch("r2g.streaming.pipeline.psycopg")
    def test_exclude_tables_filters_documents(self, mock_psycopg, simple_schema, simple_config, mock_writer):
        mock_conn = MagicMock()

        def make_cursor(*args, **kwargs):
            cursor = MagicMock()
            name = kwargs.get("name", "")
            if "users" in name:
                cursor.__iter__ = MagicMock(return_value=iter([
                    {"id": 1, "name": "Alice", "email": "a@b.com"},
                ]))
            else:
                cursor.__iter__ = MagicMock(return_value=iter([]))
            cursor.__enter__ = MagicMock(return_value=cursor)
            cursor.__exit__ = MagicMock(return_value=False)
            return cursor

        mock_conn.cursor.side_effect = make_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_psycopg.connect.return_value = mock_conn

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            exclude_tables={"orders"},
        )

        results = pipeline.run()

        doc_names = [name for name, _ in results["documents"]]
        assert "users" in doc_names
        assert "orders" not in doc_names
        assert len(results["edges"]) == 0


class TestImportErrorSurfacing:
    @patch("r2g.streaming.pipeline.psycopg")
    def test_batch_errors_captured_in_results(self, mock_psycopg, simple_schema, simple_config):
        mock_writer = MagicMock(spec=ArangoWriter)
        mock_writer.import_batch.side_effect = ImportBatchError(
            collection="users",
            error_count=2,
            total_count=5,
            details=["doc 1: unique constraint", "doc 3: invalid key"],
        )

        mock_conn = MagicMock()

        def make_cursor(*args, **kwargs):
            cursor = MagicMock()
            name = kwargs.get("name", "")
            if "users" in name:
                cursor.__iter__ = MagicMock(return_value=iter([
                    {"id": 1, "name": "Alice", "email": "a@b.com"},
                ]))
            elif "orders" in name:
                cursor.__iter__ = MagicMock(return_value=iter([
                    {"id": 10, "user_id": 1, "total": 99.99},
                ]))
            else:
                cursor.__iter__ = MagicMock(return_value=iter([]))
            cursor.__enter__ = MagicMock(return_value=cursor)
            cursor.__exit__ = MagicMock(return_value=False)
            return cursor

        mock_conn.cursor.side_effect = make_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_psycopg.connect.return_value = mock_conn

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )

        results = pipeline.run()

        assert "errors" in results
        assert "users" in results["errors"]
        assert len(results["errors"]["users"]) == 2

    @patch("r2g.streaming.pipeline.psycopg")
    def test_no_errors_key_when_clean(self, mock_psycopg, simple_schema, simple_config, mock_writer):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__iter__ = MagicMock(return_value=iter([]))
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_psycopg.connect.return_value = mock_conn

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )

        results = pipeline.run()

        assert "errors" not in results
