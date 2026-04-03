from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from r2g.connectors.arango_writer import ArangoWriter


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

        mock_coll.import_bulk.assert_called_once_with(docs, on_duplicate="replace", halt_on_error=False)
        assert result["created"] == 3

    @patch("r2g.connectors.arango_writer.ArangoClient")
    def test_empty_batch_returns_zeros(self, mock_client_cls, writer):
        mock_client_cls.return_value.db.return_value = MagicMock()
        writer.connect()
        result = writer.import_batch("users", [])
        assert result["created"] == 0


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
