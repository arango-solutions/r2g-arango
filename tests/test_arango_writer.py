from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from r2g.connectors.arango_writer import ArangoWriter, ImportBatchError


@pytest.fixture
def writer():
    return ArangoWriter(
        endpoint="http://localhost:8529",
        database="test_db",
        username="root",
        password="secret",
    )


class TestInit:
    def test_stores_connection_params(self, writer):
        assert writer.endpoint == "http://localhost:8529"
        assert writer.database_name == "test_db"
        assert writer.username == "root"
        assert writer.password == "secret"

    def test_not_connected_initially(self, writer):
        assert writer._client is None
        assert writer._db is None


class TestConnect:
    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_creates_client_and_db(self, mock_client_cls, writer):
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.db.return_value = mock_db

        result = writer.connect()

        mock_client_cls.assert_called_once_with(hosts="http://localhost:8529")
        mock_client.db.assert_called_once_with("test_db", username="root", password="secret")
        assert result is mock_db
        assert writer._db is mock_db


class TestEnsureCollection:
    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_creates_collection_when_missing(self, mock_client_cls, writer):
        mock_db = MagicMock()
        mock_db.has_collection.return_value = False
        mock_client_cls.return_value.db.return_value = mock_db

        writer.connect()
        writer.ensure_collection("users", edge=False)

        mock_db.create_collection.assert_called_once_with("users", edge=False)

    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_skips_existing_collection(self, mock_client_cls, writer):
        mock_db = MagicMock()
        mock_db.has_collection.return_value = True
        mock_client_cls.return_value.db.return_value = mock_db

        writer.connect()
        writer.ensure_collection("users", edge=False)

        mock_db.create_collection.assert_not_called()


class TestImportBatch:
    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_imports_documents(self, mock_client_cls, writer):
        mock_db = MagicMock()
        mock_coll = MagicMock()
        mock_coll.import_bulk.return_value = {"created": 3, "errors": 0, "empty": 0, "updated": 0, "ignored": 0}
        mock_db.collection.return_value = mock_coll
        mock_client_cls.return_value.db.return_value = mock_db

        writer.connect()
        docs = [{"_key": "1", "name": "a"}, {"_key": "2", "name": "b"}, {"_key": "3", "name": "c"}]
        result = writer.import_batch("users", docs)

        mock_coll.import_bulk.assert_called_once_with(
            docs, on_duplicate="replace", halt_on_error=False, details=True,
        )
        assert result["created"] == 3

    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_empty_batch_returns_zeros(self, mock_client_cls, writer):
        mock_client_cls.return_value.db.return_value = MagicMock()
        writer.connect()
        result = writer.import_batch("users", [])
        assert result["created"] == 0


class TestImportBatchErrors:
    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_raises_on_document_errors(self, mock_client_cls, writer):
        mock_db = MagicMock()
        mock_coll = MagicMock()
        mock_coll.import_bulk.return_value = {
            "created": 1, "errors": 2, "empty": 0, "updated": 0, "ignored": 0,
            "details": ["doc at pos 1: unique constraint violated", "doc at pos 2: invalid key"],
        }
        mock_db.collection.return_value = mock_coll
        mock_client_cls.return_value.db.return_value = mock_db

        writer.connect()
        with pytest.raises(ImportBatchError) as exc_info:
            writer.import_batch("users", [{"_key": "1"}, {"_key": "2"}, {"_key": "3"}])

        assert exc_info.value.error_count == 2
        assert exc_info.value.total_count == 3
        assert exc_info.value.collection == "users"
        assert len(exc_info.value.details) == 2

    def test_import_batch_error_message(self):
        err = ImportBatchError("col", error_count=5, total_count=100, details=["a", "b"])
        assert "5/100" in str(err)
        assert "col" in str(err)


class TestCreateNamedGraph:
    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_creates_graph(self, mock_client_cls, writer):
        mock_db = MagicMock()
        mock_db.has_graph.return_value = False
        mock_client_cls.return_value.db.return_value = mock_db

        writer.connect()
        edge_defs = [
            {"edge_collection": "edges", "from_vertex_collections": ["a"], "to_vertex_collections": ["b"]}
        ]
        writer.create_named_graph("my_graph", edge_defs)

        mock_db.create_graph.assert_called_once_with("my_graph", edge_definitions=edge_defs)

    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_drops_existing_graph_first(self, mock_client_cls, writer):
        mock_db = MagicMock()
        mock_db.has_graph.return_value = True
        mock_client_cls.return_value.db.return_value = mock_db

        writer.connect()
        writer.create_named_graph("my_graph", [])

        mock_db.delete_graph.assert_called_once_with("my_graph", drop_collections=False)
        mock_db.create_graph.assert_called_once()


class TestDropCollection:
    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_drops_existing(self, mock_client_cls, writer):
        mock_db = MagicMock()
        mock_db.has_collection.return_value = True
        mock_client_cls.return_value.db.return_value = mock_db

        writer.connect()
        assert writer.drop_collection("users") is True
        mock_db.delete_collection.assert_called_once_with("users")

    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_noop_when_missing(self, mock_client_cls, writer):
        mock_db = MagicMock()
        mock_db.has_collection.return_value = False
        mock_client_cls.return_value.db.return_value = mock_db

        writer.connect()
        assert writer.drop_collection("users") is False
        mock_db.delete_collection.assert_not_called()


class TestRetryLogic:
    @patch("r2g.connectors.arango_writer.time.sleep")
    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_retries_on_connection_error(self, mock_client_cls, mock_sleep):
        mock_db = MagicMock()
        mock_coll = MagicMock()
        mock_coll.import_bulk.side_effect = [
            ConnectionError("timeout"),
            {"created": 2, "errors": 0, "empty": 0, "updated": 0, "ignored": 0},
        ]
        mock_db.collection.return_value = mock_coll
        mock_client_cls.return_value.db.return_value = mock_db

        w = ArangoWriter(max_retries=3)
        w.connect()
        result = w.import_batch("test", [{"_key": "1"}])

        assert result["created"] == 2
        assert mock_coll.import_bulk.call_count == 2
        mock_sleep.assert_called_once_with(1)

    @patch("r2g.connectors.arango_writer.time.sleep")
    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_raises_after_max_retries(self, mock_client_cls, mock_sleep):
        mock_db = MagicMock()
        mock_coll = MagicMock()
        mock_coll.import_bulk.side_effect = ConnectionError("down")
        mock_db.collection.return_value = mock_coll
        mock_client_cls.return_value.db.return_value = mock_db

        w = ArangoWriter(max_retries=2)
        w.connect()
        with pytest.raises(ConnectionError):
            w.import_batch("test", [{"_key": "1"}])

        assert mock_coll.import_bulk.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("r2g.connectors.arango_writer.time.sleep")
    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_exponential_backoff(self, mock_client_cls, mock_sleep):
        mock_db = MagicMock()
        mock_coll = MagicMock()
        mock_coll.import_bulk.side_effect = [
            ConnectionError("1"),
            ConnectionError("2"),
            {"created": 1, "errors": 0, "empty": 0, "updated": 0, "ignored": 0},
        ]
        mock_db.collection.return_value = mock_coll
        mock_client_cls.return_value.db.return_value = mock_db

        w = ArangoWriter(max_retries=3)
        w.connect()
        w.import_batch("test", [{"_key": "1"}])

        assert mock_sleep.call_args_list[0][0][0] == 1  # 2^0
        assert mock_sleep.call_args_list[1][0][0] == 2  # 2^1

    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_no_retry_on_non_retryable_error(self, mock_client_cls):
        mock_db = MagicMock()
        mock_coll = MagicMock()
        mock_coll.import_bulk.side_effect = ValueError("bad data")
        mock_db.collection.return_value = mock_coll
        mock_client_cls.return_value.db.return_value = mock_db

        w = ArangoWriter(max_retries=3)
        w.connect()
        with pytest.raises(ValueError, match="bad data"):
            w.import_batch("test", [{"_key": "1"}])

        assert mock_coll.import_bulk.call_count == 1


class TestSingleDocumentOps:
    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_insert_document(self, mock_client_cls, writer):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_db = MagicMock()
        mock_client.db.return_value = mock_db
        mock_coll = MagicMock()
        mock_coll.insert.return_value = {"_key": "1", "_id": "users/1"}
        mock_db.collection.return_value = mock_coll

        writer.connect()
        result = writer.insert_document("users", {"_key": "1", "name": "Alice"})

        mock_coll.insert.assert_called_once()
        assert result["_key"] == "1"

    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_replace_document(self, mock_client_cls, writer):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_db = MagicMock()
        mock_client.db.return_value = mock_db
        mock_coll = MagicMock()
        mock_coll.replace.return_value = {"_key": "1", "_id": "users/1"}
        mock_db.collection.return_value = mock_coll

        writer.connect()
        result = writer.replace_document("users", {"_key": "1", "name": "Bob"})

        mock_coll.replace.assert_called_once()
        assert result["_key"] == "1"

    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_delete_document(self, mock_client_cls, writer):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_db = MagicMock()
        mock_client.db.return_value = mock_db
        mock_coll = MagicMock()
        mock_db.collection.return_value = mock_coll

        writer.connect()
        result = writer.delete_document("users", "1")

        mock_coll.delete.assert_called_once_with("1", ignore_missing=True)
        assert result is True

    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_apply_delta_insert(self, mock_client_cls, writer):
        from r2g.cdc.models import ArangoDelta, ArangoOperation

        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_db = MagicMock()
        mock_client.db.return_value = mock_db
        mock_coll = MagicMock()
        mock_coll.insert.return_value = {"_key": "1"}
        mock_db.collection.return_value = mock_coll
        mock_db.has_collection.return_value = True

        writer.connect()
        delta = ArangoDelta(
            operation=ArangoOperation.INSERT,
            collection="users",
            document={"_key": "1", "name": "Alice"},
        )
        writer.apply_delta(delta)
        mock_coll.insert.assert_called_once()

    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_apply_delta_delete(self, mock_client_cls, writer):
        from r2g.cdc.models import ArangoDelta, ArangoOperation

        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_db = MagicMock()
        mock_client.db.return_value = mock_db
        mock_coll = MagicMock()
        mock_db.collection.return_value = mock_coll
        mock_db.has_collection.return_value = True

        writer.connect()
        delta = ArangoDelta(
            operation=ArangoOperation.DELETE,
            collection="users",
            key="42",
        )
        writer.apply_delta(delta)
        mock_coll.delete.assert_called_once_with("42", ignore_missing=True)


class TestClose:
    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_closes_client(self, mock_client_cls, writer):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        writer.connect()
        writer.close()

        mock_client.close.assert_called_once()
        assert writer._client is None
        assert writer._db is None
