"""Streaming pipeline: reads from PostgreSQL and writes directly to ArangoDB.

Eliminates intermediate files by using server-side cursors for batched reads
and the ArangoDB HTTP bulk import API for writes.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Generator

import psycopg
from psycopg.rows import dict_row

from r2g.connectors.arango_writer import ArangoWriter
from r2g.log import get_logger
from r2g.transformers.edge_transformer import EdgeTransformer
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import MappingConfig, Schema

logger = get_logger(__name__)

ProgressFn = Callable[[str, str, int, int | None], None]


class StreamingPipeline:
    """Orchestrates streaming data from PostgreSQL to ArangoDB.

    Uses REPEATABLE READ transaction isolation for snapshot consistency
    and server-side cursors for bounded memory usage.

    When ``dry_run=True`` the pipeline connects to both PostgreSQL and
    ArangoDB (validating credentials), reads and transforms all rows,
    but skips writes and graph creation.  Returns counts and sample
    documents for pre-flight validation.

    An optional *on_progress* callback receives ``(event, name, current, total)``
    where *event* is ``"start"`` / ``"progress"`` / ``"done"``; *name* is the
    collection name; *current* is rows processed so far; *total* is the
    estimated row count (may be ``None`` if unavailable).
    """

    PREVIEW_LIMIT = 3

    def __init__(
        self,
        pg_conn_string: str,
        arango_writer: ArangoWriter,
        schema: Schema,
        config: MappingConfig,
        batch_size: int = 10_000,
        on_duplicate: str = "replace",
        pg_schema: str = "public",
        dry_run: bool = False,
        drop_collections: bool = False,
        workers: int = 1,
    ) -> None:
        self.pg_conn_string = pg_conn_string
        self.writer = arango_writer
        self.schema = schema
        self.config = config
        self.batch_size = batch_size
        self.on_duplicate = on_duplicate
        self.pg_schema = pg_schema
        self.dry_run = dry_run
        self.drop_collections = drop_collections
        self.workers = max(1, workers)
        self.previews: dict[str, list[dict[str, Any]]] = {}
        self._on_progress: ProgressFn | None = None
        self._lock = __import__("threading").Lock()

    def _notify(self, event: str, name: str, current: int, total: int | None) -> None:
        if self._on_progress is not None:
            self._on_progress(event, name, current, total)

    def _count_table_rows(self, conn: psycopg.Connection, table_name: str) -> int:
        """Fast row count within the current REPEATABLE READ transaction."""
        from psycopg.rows import tuple_row

        qualified = f"{self.pg_schema}.{table_name}"
        with conn.cursor(row_factory=tuple_row) as cur:
            cur.execute(f"SELECT count(*) FROM {qualified}")  # noqa: S608
            return cur.fetchone()[0]

    def _stream_rows(
        self,
        conn: psycopg.Connection,
        table_name: str,
    ) -> Generator[dict[str, Any], None, None]:
        """Stream rows from a PostgreSQL table using a server-side cursor."""
        qualified = f"{self.pg_schema}.{table_name}"
        cursor_name = f"r2g_{table_name}"

        with conn.cursor(name=cursor_name, row_factory=dict_row) as cur:
            cur.itersize = self.batch_size
            cur.execute(f"SELECT * FROM {qualified}")  # noqa: S608
            yield from cur

    def _make_writer(self) -> ArangoWriter:
        """Create a new ArangoWriter with the same connection params."""
        return ArangoWriter(
            endpoint=self.writer.endpoint,
            database=self.writer.database_name,
            username=self.writer.username,
            password=self.writer.password,
            max_retries=self.writer.max_retries,
        )

    def _stream_one_document(
        self,
        table_name: str,
        cm: Any,
        conn: psycopg.Connection | None = None,
        writer: ArangoWriter | None = None,
    ) -> tuple[str, int]:
        """Stream a single document collection. Thread-safe when each
        call receives its own *conn* and *writer*."""
        table_def = self.schema.tables[table_name]
        target = cm.target_collection
        w = writer or self.writer

        if not self.dry_run:
            if self.drop_collections:
                w.drop_collection(target)
            w.ensure_collection(target, edge=False)

        transformer = NodeTransformer(
            table_def,
            collection_mapping=cm,
            key_separator=self.config.key_separator,
            type_overrides=self.config.type_overrides,
        )

        assert conn is not None
        batch: list[dict[str, Any]] = []
        total = 0
        samples: list[dict[str, Any]] = []
        row_estimate = self._count_table_rows(conn, table_name)

        logger.info("stream_documents_start", table=table_name, target=target, rows=row_estimate)
        self._notify("start", target, 0, row_estimate)

        for row in self._stream_rows(conn, table_name):
            doc = transformer.transform_row(row)
            if len(samples) < self.PREVIEW_LIMIT:
                samples.append(doc)
            batch.append(doc)
            if len(batch) >= self.batch_size:
                if not self.dry_run:
                    w.import_batch(target, batch, self.on_duplicate)
                total += len(batch)
                batch.clear()
                self._notify("progress", target, total, row_estimate)

        if batch:
            if not self.dry_run:
                w.import_batch(target, batch, self.on_duplicate)
            total += len(batch)

        if self.dry_run:
            with self._lock:
                self.previews[target] = samples

        self._notify("done", target, total, row_estimate)
        logger.info("stream_documents_done", target=target, count=total)
        return (target, total)

    def _stream_one_edge(
        self,
        edge_def: Any,
        conn: psycopg.Connection | None = None,
        writer: ArangoWriter | None = None,
    ) -> tuple[str, int]:
        """Stream a single edge collection. Thread-safe when each call
        receives its own *conn* and *writer*."""
        table_name = edge_def.from_collection
        table_def = self.schema.tables[table_name]
        edge_name = edge_def.edge_collection
        w = writer or self.writer

        if not self.dry_run:
            if self.drop_collections:
                w.drop_collection(edge_name)
            w.ensure_collection(edge_name, edge=True)

        transformer = EdgeTransformer(
            edge_def,
            table_def,
            key_separator=self.config.key_separator,
        )

        assert conn is not None
        batch: list[dict[str, Any]] = []
        total = 0
        samples: list[dict[str, Any]] = []
        row_estimate = self._count_table_rows(conn, table_name)

        logger.info("stream_edges_start", table=table_name, edge=edge_name, rows=row_estimate)
        self._notify("start", edge_name, 0, row_estimate)

        for row in self._stream_rows(conn, table_name):
            doc = transformer.transform_row(row)
            if doc is not None:
                if len(samples) < self.PREVIEW_LIMIT:
                    samples.append(doc)
                batch.append(doc)
                if len(batch) >= self.batch_size:
                    if not self.dry_run:
                        w.import_batch(edge_name, batch, self.on_duplicate)
                    total += len(batch)
                    batch.clear()
                    self._notify("progress", edge_name, total, row_estimate)

        if batch:
            if not self.dry_run:
                w.import_batch(edge_name, batch, self.on_duplicate)
            total += len(batch)

        if self.dry_run:
            with self._lock:
                self.previews[edge_name] = samples

        self._notify("done", edge_name, total, row_estimate)
        logger.info("stream_edges_done", edge=edge_name, count=total)
        return (edge_name, total)

    def _stream_documents(
        self,
        conn: psycopg.Connection,
    ) -> list[tuple[str, int]]:
        """Stream all document collections (sequential, single connection)."""
        results: list[tuple[str, int]] = []
        for _key, cm in self.config.collections.items():
            if cm.collection_type != "document":
                continue
            if cm.source_table not in self.schema.tables:
                logger.warning("stream_skip_unknown_table", table=cm.source_table)
                continue
            results.append(self._stream_one_document(cm.source_table, cm, conn=conn))
        return results

    def _stream_edges(
        self,
        conn: psycopg.Connection,
    ) -> list[tuple[str, int]]:
        """Stream all edge collections (sequential, single connection)."""
        results: list[tuple[str, int]] = []
        for edge_def in self.config.edges:
            if edge_def.from_collection not in self.schema.tables:
                logger.warning("stream_edge_skip_unknown_table", table=edge_def.from_collection)
                continue
            results.append(self._stream_one_edge(edge_def, conn=conn))
        return results

    def _run_parallel_phase(
        self,
        jobs: list[tuple[Any, ...]],
        phase_fn: str,
    ) -> list[tuple[str, int]]:
        """Run *jobs* in parallel, each on its own PG conn + ArangoDB writer.

        *phase_fn* is ``"doc"`` or ``"edge"``; each job tuple contains
        the arguments after (conn, writer) for the target method.
        """
        results: list[tuple[str, int]] = []

        def worker(job_args: tuple) -> tuple[str, int]:
            w = self._make_writer()
            if not self.dry_run:
                w.connect()
            with psycopg.connect(
                self.pg_conn_string, row_factory=dict_row, autocommit=False
            ) as c:
                c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                if phase_fn == "doc":
                    result = self._stream_one_document(job_args[0], job_args[1], conn=c, writer=w)
                else:
                    result = self._stream_one_edge(job_args[0], conn=c, writer=w)
            if not self.dry_run:
                w.close()
            return result

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = {pool.submit(worker, j): j for j in jobs}
            for future in as_completed(futures):
                results.append(future.result())

        return results

    def _run_parallel(
        self,
        graph_name: str | None,
    ) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
        """Execute documents then edges in parallel using separate connections."""
        logger.info("parallel_streaming", workers=self.workers)

        doc_jobs: list[tuple[Any, ...]] = []
        for _key, cm in self.config.collections.items():
            if cm.collection_type != "document":
                continue
            if cm.source_table not in self.schema.tables:
                continue
            doc_jobs.append((cm.source_table, cm))

        edge_jobs: list[tuple[Any, ...]] = []
        for edge_def in self.config.edges:
            if edge_def.from_collection not in self.schema.tables:
                continue
            edge_jobs.append((edge_def,))

        doc_results = self._run_parallel_phase(doc_jobs, "doc")
        edge_results = self._run_parallel_phase(edge_jobs, "edge")
        return doc_results, edge_results

    def run(
        self,
        graph_name: str | None = None,
        on_progress: ProgressFn | None = None,
    ) -> dict[str, list[tuple[str, int]]]:
        """Execute the full streaming pipeline.

        Opens a single PG connection with REPEATABLE READ isolation
        for consistent snapshot semantics, then streams documents
        followed by edges into ArangoDB.

        When ``dry_run`` is True the pipeline connects to both PG and
        ArangoDB (validating credentials and reachability) and reads /
        transforms every row, but skips all writes.  Sample documents
        are stored in ``self.previews``.

        *on_progress* receives ``(event, collection, current, total)``
        callbacks for Rich progress bars or other UIs.

        Returns a dict with 'documents', 'edges', and 'elapsed_seconds' keys.
        """
        self._on_progress = on_progress
        t0 = time.monotonic()
        self.writer.connect()

        if self.workers > 1:
            doc_results, edge_results = self._run_parallel(graph_name)
        else:
            with psycopg.connect(
                self.pg_conn_string,
                row_factory=dict_row,
                autocommit=False,
            ) as conn:
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                logger.info("pg_snapshot_started", isolation="REPEATABLE READ")

                doc_results = self._stream_documents(conn)
                edge_results = self._stream_edges(conn)

        if not self.dry_run and graph_name:
            edge_defs = []
            for edge_def in self.config.edges:
                edge_defs.append({
                    "edge_collection": edge_def.edge_collection,
                    "from_vertex_collections": [edge_def.from_collection],
                    "to_vertex_collections": [edge_def.to_collection],
                })
            self.writer.create_named_graph(graph_name, edge_defs)

        self.writer.close()
        elapsed = time.monotonic() - t0

        return {
            "documents": doc_results,
            "edges": edge_results,
            "elapsed_seconds": elapsed,
        }
