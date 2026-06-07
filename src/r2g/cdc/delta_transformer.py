"""Transform CDC ChangeEvents into ArangoDB deltas.

Reuses the existing NodeTransformer and EdgeTransformer to produce
properly typed, correctly keyed documents and edges from row-level
change events.
"""

from __future__ import annotations

from typing import Any

from r2g.cdc.models import (
    ArangoDelta,
    ArangoOperation,
    ChangeEvent,
    ChangeOperation,
)
from r2g.keys import sanitize_key_component
from r2g.log import get_logger
from r2g.transformers.edge_transformer import EdgeTransformer
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import EdgeDefinition, MappingConfig, Schema

logger = get_logger(__name__)

_OP_MAP = {
    ChangeOperation.INSERT: ArangoOperation.INSERT,
    ChangeOperation.UPDATE: ArangoOperation.REPLACE,
    ChangeOperation.DELETE: ArangoOperation.DELETE,
}


class DeltaTransformer:
    """Convert a stream of ChangeEvents into ArangoDeltas.

    For each change event the transformer:
    1. Looks up the target collection mapping for the source table.
    2. Transforms the row through NodeTransformer (for documents).
    3. Finds all edges sourced from the affected table and builds
       edge deltas through EdgeTransformer.

    On DELETE the transformer emits delete operations for both the
    document and any edges that originated from the deleted row.
    On UPDATE edges are replaced to reflect new FK values.
    """

    def __init__(
        self,
        schema: Schema,
        config: MappingConfig,
    ) -> None:
        self.schema = schema
        self.config = config
        self._cm_by_table = {
            cm.source_table: (key, cm)
            for key, cm in config.collections.items()
        }
        self._edges_by_source: dict[str, list[EdgeDefinition]] = {}
        for edge in config.edges:
            self._edges_by_source.setdefault(edge.from_collection, []).append(edge)

    def _build_node_transformer(self, table_name: str) -> NodeTransformer | None:
        table_def = self.schema.tables.get(table_name)
        mapping_entry = self._cm_by_table.get(table_name)
        if table_def is None or mapping_entry is None:
            return None
        _, cm = mapping_entry
        return NodeTransformer(
            table_def,
            collection_mapping=cm,
            key_separator=self.config.key_separator,
            type_overrides=self.config.type_overrides,
        )

    def _build_edge_transformer(
        self, table_name: str, edge_def: EdgeDefinition
    ) -> EdgeTransformer | None:
        table_def = self.schema.tables.get(table_name)
        if table_def is None:
            return None
        from r2g.config import ConfigManager

        target_by_source = ConfigManager.target_by_source_table(self.config)
        return EdgeTransformer(
            edge_def,
            table_def,
            key_separator=self.config.key_separator,
            from_name=target_by_source.get(edge_def.from_collection),
            to_name=target_by_source.get(edge_def.to_collection),
        )

    def _document_key_from_row(
        self, table_name: str, row: dict[str, Any]
    ) -> str | None:
        table_def = self.schema.tables.get(table_name)
        if table_def is None or not table_def.primary_key:
            return None
        parts = []
        for pk in table_def.primary_key:
            val = row.get(pk)
            if val is None:
                return None
            parts.append(sanitize_key_component(val))
        return self.config.key_separator.join(parts)

    def transform(self, event: ChangeEvent) -> list[ArangoDelta]:
        """Produce zero or more ArangoDeltas from a single ChangeEvent."""
        deltas: list[ArangoDelta] = []
        table_name = event.table_name

        mapping_entry = self._cm_by_table.get(table_name)
        if mapping_entry is None:
            logger.debug("cdc_skip_unmapped_table", table=table_name)
            return deltas

        _, cm = mapping_entry
        target = cm.target_collection
        arango_op = _OP_MAP[event.operation]

        if event.is_delete:
            doc_key = self._document_key_from_row(table_name, event.old_row or {})
            if doc_key:
                deltas.append(ArangoDelta(
                    operation=ArangoOperation.DELETE,
                    collection=target,
                    key=doc_key,
                ))
            self._emit_edge_deletes(deltas, table_name, event.old_row or {})
        else:
            node_xform = self._build_node_transformer(table_name)
            if node_xform is None:
                return deltas
            row = event.new_row or {}
            doc = node_xform.transform_row(row)
            deltas.append(ArangoDelta(
                operation=arango_op,
                collection=target,
                document=doc,
                key=doc.get("_key"),
            ))
            self._emit_edge_upserts(deltas, table_name, row, arango_op)

        return deltas

    def _emit_edge_upserts(
        self,
        deltas: list[ArangoDelta],
        table_name: str,
        row: dict[str, Any],
        op: ArangoOperation,
    ) -> None:
        edge_defs = self._edges_by_source.get(table_name, [])
        for edge_def in edge_defs:
            edge_xform = self._build_edge_transformer(table_name, edge_def)
            if edge_xform is None:
                continue
            edge_doc = edge_xform.transform_row(row)
            if edge_doc is not None:
                edge_op = ArangoOperation.REPLACE if op == ArangoOperation.REPLACE else ArangoOperation.INSERT
                deltas.append(ArangoDelta(
                    operation=edge_op,
                    collection=edge_def.edge_collection,
                    is_edge=True,
                    document=edge_doc,
                    key=edge_doc.get("_key"),
                ))

    def _emit_edge_deletes(
        self,
        deltas: list[ArangoDelta],
        table_name: str,
        old_row: dict[str, Any],
    ) -> None:
        edge_defs = self._edges_by_source.get(table_name, [])
        for edge_def in edge_defs:
            edge_xform = self._build_edge_transformer(table_name, edge_def)
            if edge_xform is None:
                continue
            edge_doc = edge_xform.transform_row(old_row)
            if edge_doc is not None:
                deltas.append(ArangoDelta(
                    operation=ArangoOperation.DELETE,
                    collection=edge_def.edge_collection,
                    is_edge=True,
                    key=edge_doc.get("_key"),
                ))
