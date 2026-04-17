from __future__ import annotations

from unittest.mock import MagicMock, patch

from r2g.connectors.arango_reader import ArangoIntrospector


def _make_mock_collection(name: str, docs: list[dict], *, count: int | None = None):
    coll = MagicMock()
    coll.name = name
    coll.count.return_value = count if count is not None else len(docs)
    coll.find.return_value = iter(docs)
    return coll


def _make_mock_db(collections_info, collection_map, graphs_info):
    """Build a mock StandardDatabase.

    Args:
        collections_info: list of dicts returned by db.collections()
        collection_map: dict mapping collection name -> mock collection object
        graphs_info: list of dicts with name + edge_definitions
    """
    db = MagicMock()
    db.collections.return_value = collections_info
    db.collection.side_effect = lambda name: collection_map[name]

    mock_graphs = []
    for g_info in graphs_info:
        mock_graph = MagicMock()
        mock_graph.edge_definitions.return_value = g_info["edge_definitions"]
        mock_graphs.append(g_info)

    db.graphs.return_value = [{"name": g["name"]} for g in graphs_info]

    graph_map = {}
    for g_info in graphs_info:
        mg = MagicMock()
        mg.edge_definitions.return_value = g_info["edge_definitions"]
        graph_map[g_info["name"]] = mg

    db.graph.side_effect = lambda name: graph_map[name]
    return db


class TestArangoIntrospector:
    def test_introspect_doc_and_edge_collections(self):
        users_coll = _make_mock_collection("users", [
            {"_key": "1", "_id": "users/1", "_rev": "r1", "name": "Alice", "email": "a@b.com"},
            {"_key": "2", "_id": "users/2", "_rev": "r2", "name": "Bob", "age": 30},
        ], count=100)

        orders_coll = _make_mock_collection("orders", [
            {"_key": "o1", "_id": "orders/o1", "_rev": "r1", "total": 99.99, "status": "shipped"},
        ], count=50)

        follows_coll = _make_mock_collection("follows", [
            {"_key": "f1", "_id": "follows/f1", "_rev": "r1", "_from": "users/1", "_to": "users/2", "since": "2024"},
        ], count=200)

        collections_info = [
            {"name": "users", "system": False, "type": 2},
            {"name": "orders", "system": False, "type": 2},
            {"name": "follows", "system": False, "type": 3},
            {"name": "_system_col", "system": True, "type": 2},
        ]
        collection_map = {"users": users_coll, "orders": orders_coll, "follows": follows_coll}

        graphs_info = [{
            "name": "social",
            "edge_definitions": [{
                "edge_collection": "follows",
                "from_vertex_collections": ["users"],
                "to_vertex_collections": ["users"],
            }],
        }]

        mock_db = _make_mock_db(collections_info, collection_map, graphs_info)

        with patch("r2g.connectors.arango_reader.ArangoClient") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value = mock_client
            mock_client.db.return_value = mock_db

            intro = ArangoIntrospector(endpoint="http://test:8529", database="testdb")
            result = intro.introspect()

        assert len(result["document_collections"]) == 2
        assert len(result["edge_collections"]) == 1

        users_entry = next(c for c in result["document_collections"] if c["name"] == "users")
        assert users_entry["count"] == 100
        assert "name" in users_entry["properties"]
        assert "email" in users_entry["properties"]
        assert "age" in users_entry["properties"]
        assert "_key" not in users_entry["properties"]

        follows_entry = result["edge_collections"][0]
        assert follows_entry["name"] == "follows"
        assert follows_entry["count"] == 200
        assert "since" in follows_entry["properties"]

        assert len(result["graphs"]) == 1
        assert result["graphs"][0]["name"] == "social"
        assert result["graphs"][0]["edge_definitions"][0]["edge_collection"] == "follows"

        mock_client.close.assert_called_once()

    def test_introspect_empty_database(self):
        mock_db = _make_mock_db([], {}, [])

        with patch("r2g.connectors.arango_reader.ArangoClient") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value = mock_client
            mock_client.db.return_value = mock_db

            intro = ArangoIntrospector()
            result = intro.introspect()

        assert result["document_collections"] == []
        assert result["edge_collections"] == []
        assert result["graphs"] == []

    def test_introspect_skips_system_collections(self):
        collections_info = [
            {"name": "_analyzers", "system": True, "type": 2},
            {"name": "_graphs", "system": True, "type": 2},
        ]
        mock_db = _make_mock_db(collections_info, {}, [])

        with patch("r2g.connectors.arango_reader.ArangoClient") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value = mock_client
            mock_client.db.return_value = mock_db

            intro = ArangoIntrospector()
            result = intro.introspect()

        assert result["document_collections"] == []
        assert result["edge_collections"] == []

    def test_introspect_multiple_graphs(self):
        coll_a = _make_mock_collection("knows", [{"weight": 0.5}], count=10)
        coll_b = _make_mock_collection("likes", [{"score": 3}], count=20)

        collections_info = [
            {"name": "knows", "system": False, "type": 3},
            {"name": "likes", "system": False, "type": 3},
        ]
        collection_map = {"knows": coll_a, "likes": coll_b}

        graphs_info = [
            {
                "name": "social",
                "edge_definitions": [{
                    "edge_collection": "knows",
                    "from_vertex_collections": ["people"],
                    "to_vertex_collections": ["people"],
                }],
            },
            {
                "name": "preferences",
                "edge_definitions": [{
                    "edge_collection": "likes",
                    "from_vertex_collections": ["people"],
                    "to_vertex_collections": ["items"],
                }],
            },
        ]

        mock_db = _make_mock_db(collections_info, collection_map, graphs_info)

        with patch("r2g.connectors.arango_reader.ArangoClient") as MockClient:
            mock_client = MagicMock()
            MockClient.return_value = mock_client
            mock_client.db.return_value = mock_db

            result = ArangoIntrospector().introspect()

        assert len(result["graphs"]) == 2
        graph_names = {g["name"] for g in result["graphs"]}
        assert graph_names == {"social", "preferences"}


class TestSampleProperties:
    def test_extracts_non_system_keys(self):
        coll = _make_mock_collection("test", [
            {"_key": "1", "_id": "test/1", "_rev": "r1", "name": "Alice", "age": 30},
            {"_key": "2", "_id": "test/2", "_rev": "r2", "email": "a@b.com"},
        ])
        props = ArangoIntrospector._sample_properties(coll)
        assert props == ["age", "email", "name"]

    def test_empty_collection(self):
        coll = _make_mock_collection("empty", [])
        props = ArangoIntrospector._sample_properties(coll)
        assert props == []

    def test_handles_exception(self):
        coll = MagicMock()
        coll.name = "broken"
        coll.find.side_effect = RuntimeError("connection lost")
        props = ArangoIntrospector._sample_properties(coll)
        assert props == []

    def test_respects_limit(self):
        coll = MagicMock()
        coll.name = "test"
        coll.find.return_value = iter([{"x": 1}])
        ArangoIntrospector._sample_properties(coll, limit=10)
        coll.find.assert_called_once_with({}, limit=10)
