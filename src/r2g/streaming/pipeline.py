"""Streaming pipeline: reads from PostgreSQL and writes directly to ArangoDB.

Eliminates intermediate files by using server-side cursors for batched reads
and the ArangoDB HTTP bulk import API for writes.
"""

from __future__ import annotations

from typing import Any, Generator

import psycopg
from psycopg.rows import dict_row

from r2g.config import ConfigManager, pg_type_to_json_type
from r2g.connectors.arango_writer import ArangoWriter
from r2g.log import get_logger
from r2g.transformers.edge_transformer import EdgeTransformer
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import EdgeDefinition, MappingConfig, Schema

logger = get_logger(__name__)


class StreamingPipeline:
    """Orchestrates streaming data from PostgreSQL to ArangoDB.

    Uses REPEATABLE READ transaction isolation for snapshot consistency
    and server-side cursors for bounded memory usage.
    """

    def __init__(
        self,
        pg_conn_string: str,
        arango_writer: ArangoWriter,
        schema: Schema,
        config: MappingConfig,
        batch_size: int = 10_000,
        on_duplicate: str = "replace",
        pg_schema: str = "public",
    ) -> None:
        self.pg_conn_string = pg_conn_string
        self.writer = arango_writer
        self.schema = schema
        self.config = config
        self.batch_size = batch_size
        self.on_duplicate = on_duplicate
        self.pg_schema = pg_schema

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

    def _stream_documents(
        self,
        conn: psycopg.Connection,
    ) -> list[tuple[str, int]]:
        """Stream all document collections from PG to ArangoDB."""
        results: list[tuple[str, int]] = []

        for _key, cm in self.config.collections.items():
            if cm.collection_type != "document":
                continue
            table_name = cm.source_table
            if table_name not in self.schema.tables:
                logger.warning("stream_skip_unknown_table", table=table_name)
                continue

            table_def = self.schema.tables[table_name]
            target = cm.target_collection

            self.writer.ensure_collection(target, edge=False)

            transformer = NodeTransformer(
                table_def,
                collection_mapping=cm,
                key_separator=self.config.key_separator,
                type_overrides=self.config.type_overrides,
            )

            batch: list[dict[str, Any]] = []
            total = 0

            logger.info("stream_documents_start", table=table_name, target=target)

            for row in self._stream_rows(conn, table_name):
                doc = transformer.transform_row(row)
                batch.append(doc)
                if len(batch) >= self.batch_size:
                    self.writer.import_batch(target, batch, self.on_duplicate)
                    total += len(batch)
                    batch.clear()

            if batch:
                self.writer.import_batch(target, batch, self.on_duplicate)
                total += len(batch)

            logger.info("stream_documents_done", target=target, count=total)
            results.append((target, total))

        return results

    def _stream_edges(
        self,
        conn: psycopg.Connection,
    ) -> list[tuple[str, int]]:
        """Stream all edge collections from PG to ArangoDB."""
        results: list[tuple[str, int]] = []

        for edge_def in self.config.edges:
            table_name = edge_def.from_collection
            if table_name not in self.schema.tables:
                logger.warning("stream_edge_skip_unknown_table", table=table_name)
                continue

            table_def = self.schema.tables[table_name]
            self.writer.ensure_collection(edge_def.edge_collection, edge=True)

            transformer = EdgeTransformer(
                edge_def,
                table_def,
                key_separator=self.config.key_separator,
            )

            batch: list[dict[str, Any]] = []
            total = 0

            logger.info(
                "stream_edges_start",
                table=table_name,
                edge=edge_def.edge_collection,
            )

            for row in self._stream_rows(conn, table_name):
                doc = transformer.transform_row(row)
                if doc is not None:
                    batch.append(doc)
                    if len(batch) >= self.batch_size:
                        self.writer.import_batch(
                            edge_def.edge_collection, batch, self.on_duplicate
                        )
                        total += len(batch)
                        batch.clear()

            if batch:
                self.writer.import_batch(
                    edge_def.edge_collection, batch, self.on_duplicate
                )
                total += len(batch)

            logger.info(
                "stream_edges_done",
                edge=edge_def.edge_collection,
                count=total,
            )
            results.append((edge_def.edge_collection, total))

        return results

    def run(
        self,
        graph_name: str | None = None,
    ) -> dict[str, list[tuple[str, int]]]:
        """Execute the full streaming pipeline.

        Opens a single PG connection with REPEATABLE READ isolation
        for consistent snapshot semantics, then streams documents
        followed by edges into ArangoDB.

        Returns a dict with 'documents' and 'edges' result lists.
        """
        self.writer.connect()

        with psycopg.connect(
            self.pg_conn_string,
            row_factory=dict_row,
            autocommit=False,
        ) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            logger.info("pg_snapshot_started", isolation="REPEATABLE READ")

            doc_results = self._stream_documents(conn)
            edge_results = self._stream_edges(conn)

        if graph_name:
            edge_defs = []
            for edge_def in self.config.edges:
                edge_defs.append({
                    "edge_collection": edge_def.edge_collection,
                    "from_vertex_collections": [edge_def.from_collection],
                    "to_vertex_collections": [edge_def.to_collection],
                })
            self.writer.create_named_graph(graph_name, edge_defs)

        self.writer.close()

        return {"documents": doc_results, "edges": edge_results}
