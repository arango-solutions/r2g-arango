"""ArangoDB HTTP API writer for streaming bulk imports.

Uses python-arango to batch-insert documents and edges
without writing intermediate files to disk.
"""

from __future__ import annotations

from typing import Any, Sequence

from arango import ArangoClient
from arango.database import StandardDatabase

from r2g.log import get_logger

logger = get_logger(__name__)


class ArangoWriter:
    """Connects to ArangoDB and bulk-imports documents via the HTTP API."""

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
        self._client: ArangoClient | None = None
        self._db: StandardDatabase | None = None

    def connect(self) -> StandardDatabase:
        self._client = ArangoClient(hosts=self.endpoint)
        self._db = self._client.db(
            self.database_name,
            username=self.username,
            password=self.password,
        )
        logger.info(
            "arango_connected",
            endpoint=self.endpoint,
            database=self.database_name,
        )
        return self._db

    @property
    def db(self) -> StandardDatabase:
        if self._db is None:
            return self.connect()
        return self._db

    def ensure_collection(
        self, name: str, edge: bool = False
    ) -> None:
        if not self.db.has_collection(name):
            self.db.create_collection(name, edge=edge)
            logger.info("arango_collection_created", name=name, edge=edge)

    def import_batch(
        self,
        collection_name: str,
        documents: Sequence[dict[str, Any]],
        on_duplicate: str = "replace",
    ) -> dict[str, int]:
        """Bulk-import a batch of documents into a collection.

        Returns a dict with keys 'created', 'errors', 'empty', 'updated', 'ignored'.
        """
        if not documents:
            return {"created": 0, "errors": 0, "empty": 0, "updated": 0, "ignored": 0}

        coll = self.db.collection(collection_name)
        result = coll.import_bulk(
            documents,
            on_duplicate=on_duplicate,
            halt_on_error=False,
        )
        logger.debug(
            "arango_batch_imported",
            collection=collection_name,
            count=len(documents),
            created=result.get("created", 0),
            errors=result.get("errors", 0),
        )
        return result

    def create_named_graph(
        self,
        graph_name: str,
        edge_definitions: list[dict[str, Any]],
    ) -> None:
        """Create a named graph with the given edge definitions.

        Each edge_definition dict should have keys:
          edge_collection, from_vertex_collections, to_vertex_collections
        """
        if self.db.has_graph(graph_name):
            self.db.delete_graph(graph_name, drop_collections=False)
            logger.info("arango_graph_dropped", name=graph_name)

        self.db.create_graph(graph_name, edge_definitions=edge_definitions)
        logger.info("arango_graph_created", name=graph_name)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self._db = None
