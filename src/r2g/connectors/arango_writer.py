"""ArangoDB HTTP API writer for streaming bulk imports.

Uses python-arango to batch-insert documents and edges
without writing intermediate files to disk.
"""

from __future__ import annotations

import time
from typing import Any, Sequence

from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import (
    ArangoServerError,
    ServerConnectionError,
)

from r2g.log import get_logger

logger = get_logger(__name__)

RETRYABLE_HTTP_CODES = {408, 429, 500, 502, 503, 504}


class ImportBatchError(Exception):
    """Raised when an ``import_bulk`` call returns document-level errors."""

    def __init__(
        self,
        collection: str,
        error_count: int,
        total_count: int,
        details: list[str] | None = None,
    ) -> None:
        self.collection = collection
        self.error_count = error_count
        self.total_count = total_count
        self.details = details or []
        super().__init__(
            f"{error_count}/{total_count} documents failed to import "
            f"into '{collection}'"
        )


class ArangoWriter:
    """Connects to ArangoDB and bulk-imports documents via the HTTP API.

    Batch imports are retried with exponential backoff on transient
    server errors (408, 429, 5xx) and connection failures.
    """

    def __init__(
        self,
        endpoint: str = "http://localhost:8529",
        database: str = "_system",
        username: str = "root",
        password: str = "",
        max_retries: int = 3,
    ) -> None:
        self.endpoint = endpoint
        self.database_name = database
        self.username = username
        self.password = password
        self.max_retries = max_retries
        self._client: ArangoClient | None = None
        self._db: StandardDatabase | None = None

    def ensure_database(self) -> None:
        """Create the target database if it does not already exist.

        Connects to the ``_system`` database with the same credentials to
        check for and, if necessary, create the target database. No-op when
        the target is ``_system`` itself (which always exists).
        """
        if not self.database_name or self.database_name == "_system":
            return
        client = ArangoClient(hosts=self.endpoint)
        try:
            sys_db = client.db(
                "_system",
                username=self.username,
                password=self.password,
            )
            if not sys_db.has_database(self.database_name):
                sys_db.create_database(self.database_name)
                logger.info("arango_database_created", name=self.database_name)
        finally:
            client.close()

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

    def drop_collection(self, name: str) -> bool:
        """Drop a collection if it exists. Returns True if dropped."""
        if self.db.has_collection(name):
            self.db.delete_collection(name)
            logger.info("arango_collection_dropped", name=name)
            return True
        return False

    def ensure_collection(
        self, name: str, edge: bool = False
    ) -> None:
        if not self.db.has_collection(name):
            self.db.create_collection(name, edge=edge)
            logger.info("arango_collection_created", name=name, edge=edge)

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        if isinstance(exc, (ServerConnectionError, ConnectionError, OSError)):
            return True
        if isinstance(exc, ArangoServerError):
            return getattr(exc, "http_code", 0) in RETRYABLE_HTTP_CODES
        return False

    def import_batch(
        self,
        collection_name: str,
        documents: Sequence[dict[str, Any]],
        on_duplicate: str = "replace",
    ) -> dict[str, int]:
        """Bulk-import a batch of documents into a collection.

        Retries up to ``max_retries`` times on transient server errors
        with exponential backoff (1s, 2s, 4s, ...).

        Returns a dict with keys 'created', 'errors', 'empty', 'updated', 'ignored'.

        Raises :class:`ImportBatchError` if the result contains document-level
        errors (``errors > 0``), wrapping the details from ArangoDB.
        """
        if not documents:
            return {"created": 0, "errors": 0, "empty": 0, "updated": 0, "ignored": 0}

        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                coll = self.db.collection(collection_name)
                result = coll.import_bulk(
                    documents,
                    on_duplicate=on_duplicate,
                    halt_on_error=False,
                    details=True,
                )
                error_count = result.get("errors", 0)
                logger.debug(
                    "arango_batch_imported",
                    collection=collection_name,
                    count=len(documents),
                    created=result.get("created", 0),
                    errors=error_count,
                )
                if error_count > 0:
                    details = result.get("details", [])
                    raise ImportBatchError(
                        collection=collection_name,
                        error_count=error_count,
                        total_count=len(documents),
                        details=details,
                    )
                return result
            except ImportBatchError:
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < self.max_retries and self._is_retryable(exc):
                    wait = 2**attempt
                    logger.warning(
                        "arango_batch_retry",
                        collection=collection_name,
                        attempt=attempt + 1,
                        wait_seconds=wait,
                        error=str(exc),
                    )
                    time.sleep(wait)
                else:
                    raise
        raise last_exc  # type: ignore[misc]

    def insert_document(
        self,
        collection_name: str,
        document: dict[str, Any],
    ) -> dict[str, Any]:
        """Insert a single document. Retries on transient errors."""
        return self._single_doc_op(collection_name, document, "insert")

    def replace_document(
        self,
        collection_name: str,
        document: dict[str, Any],
    ) -> dict[str, Any]:
        """Replace a single document (by _key). Retries on transient errors."""
        return self._single_doc_op(collection_name, document, "replace")

    def delete_document(
        self,
        collection_name: str,
        key: str,
    ) -> bool:
        """Delete a single document by _key. Returns True if deleted."""
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                coll = self.db.collection(collection_name)
                coll.delete(key, ignore_missing=True)
                logger.debug(
                    "arango_doc_deleted",
                    collection=collection_name,
                    key=key,
                )
                return True
            except Exception as exc:
                last_exc = exc
                if attempt < self.max_retries and self._is_retryable(exc):
                    wait = 2**attempt
                    logger.warning(
                        "arango_delete_retry",
                        collection=collection_name,
                        key=key,
                        attempt=attempt + 1,
                        wait_seconds=wait,
                    )
                    time.sleep(wait)
                else:
                    raise
        raise last_exc  # type: ignore[misc]

    def _single_doc_op(
        self,
        collection_name: str,
        document: dict[str, Any],
        op: str,
    ) -> dict[str, Any]:
        """Execute a single-document insert or replace with retry."""
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                coll = self.db.collection(collection_name)
                if op == "insert":
                    result = coll.insert(document, overwrite=False, silent=False)
                else:
                    result = coll.replace(document, silent=False)
                logger.debug(
                    f"arango_doc_{op}",
                    collection=collection_name,
                    key=document.get("_key"),
                )
                return result  # type: ignore[return-value]
            except Exception as exc:
                last_exc = exc
                if attempt < self.max_retries and self._is_retryable(exc):
                    wait = 2**attempt
                    logger.warning(
                        f"arango_{op}_retry",
                        collection=collection_name,
                        attempt=attempt + 1,
                        wait_seconds=wait,
                    )
                    time.sleep(wait)
                else:
                    raise
        raise last_exc  # type: ignore[misc]

    def apply_delta(
        self,
        delta: Any,
    ) -> None:
        """Apply a single ArangoDelta to ArangoDB.

        Dispatches to insert_document, replace_document, or delete_document
        based on the delta's operation field.
        """
        from r2g.cdc.models import ArangoOperation

        self.ensure_collection(delta.collection, edge=delta.is_edge)

        if delta.operation == ArangoOperation.INSERT:
            self.insert_document(delta.collection, delta.document)
        elif delta.operation == ArangoOperation.REPLACE:
            self.replace_document(delta.collection, delta.document)
        elif delta.operation == ArangoOperation.DELETE:
            if delta.effective_key:
                self.delete_document(delta.collection, delta.effective_key)

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
