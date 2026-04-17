"""ArangoDB schema introspection for target graph discovery."""

from __future__ import annotations

from typing import Any

from arango import ArangoClient
from arango.database import StandardDatabase

from r2g.log import get_logger

logger = get_logger(__name__)


class ArangoIntrospector:
    """Connects to ArangoDB and discovers the existing schema."""

    def __init__(
        self,
        endpoint: str = "http://localhost:8529",
        database: str = "_system",
        username: str = "root",
        password: str = "",
    ) -> None:
        self.endpoint = endpoint
        self.database_name = database
        self.username = username
        self.password = password

    def introspect(self) -> dict[str, Any]:
        """Return the full graph schema of the target database.

        Returns a dict with:
        - document_collections: list of {name, count, properties: list[str]}
        - edge_collections: list of {name, count, properties: list[str]}
        - graphs: list of {name, edge_definitions}
        """
        client = ArangoClient(hosts=self.endpoint)
        db = client.db(self.database_name, username=self.username, password=self.password)
        try:
            return self._build_schema(db)
        finally:
            client.close()

    def _build_schema(self, db: StandardDatabase) -> dict[str, Any]:
        """Build schema by inspecting all collections and graphs."""
        doc_collections: list[dict[str, Any]] = []
        edge_collections: list[dict[str, Any]] = []

        for coll_info in db.collections():
            if coll_info["system"]:
                continue
            name = coll_info["name"]
            is_edge = coll_info["type"] == 3
            coll = db.collection(name)
            count = coll.count()

            properties = self._sample_properties(coll)

            entry: dict[str, Any] = {
                "name": name,
                "count": count,
                "properties": properties,
            }

            if is_edge:
                edge_collections.append(entry)
            else:
                doc_collections.append(entry)

        graphs: list[dict[str, Any]] = []
        for graph in db.graphs():
            graph_name = graph["name"]
            g = db.graph(graph_name)
            edge_defs = []
            for ed in g.edge_definitions():
                edge_defs.append({
                    "edge_collection": ed["edge_collection"],
                    "from_vertex_collections": ed["from_vertex_collections"],
                    "to_vertex_collections": ed["to_vertex_collections"],
                })
            graphs.append({"name": graph_name, "edge_definitions": edge_defs})

        return {
            "document_collections": doc_collections,
            "edge_collections": edge_collections,
            "graphs": graphs,
        }

    @staticmethod
    def _sample_properties(coll: Any, limit: int = 5) -> list[str]:
        """Sample documents to discover property names (excluding system attrs)."""
        try:
            cursor = coll.find({}, limit=limit)
            props: set[str] = set()
            for doc in cursor:
                for key in doc:
                    if not key.startswith("_"):
                        props.add(key)
            return sorted(props)
        except Exception:
            logger.debug("sample_properties_failed", collection=coll.name)
            return []
