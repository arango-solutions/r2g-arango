from __future__ import annotations

from collections.abc import Generator, Iterable
from typing import Any, Dict, Optional

from r2g.log import get_logger
from r2g.types import CollectionMapping, EdgeDefinition, Schema, Table

logger = get_logger(__name__)


class EdgeTransformer:
    def __init__(
        self,
        edge_def: EdgeDefinition,
        source_table: Table,
        *,
        key_separator: str = "_",
        join_mode: bool = False,
    ) -> None:
        self.edge_def = edge_def
        self.source_table = source_table
        self.key_separator = key_separator
        self.join_mode = join_mode

    @classmethod
    def for_join_table(
        cls,
        table: Table,
        collection_mapping: CollectionMapping,
        schema: Schema,
        key_separator: str = "_",
    ) -> EdgeTransformer:
        if len(table.foreign_keys) != 2:
            raise ValueError(
                f"Join table '{table.name}' must have exactly 2 foreign keys, got {len(table.foreign_keys)}"
            )
        fks = sorted(table.foreign_keys, key=lambda fk: (fk.foreign_table, fk.columns[0]))
        fk_a, fk_b = fks
        for fk in (fk_a, fk_b):
            if fk.foreign_table not in schema.tables:
                logger.warning(
                    "join_fk_unknown_referenced_table",
                    join_table=table.name,
                    referenced=fk.foreign_table,
                )
        edge_def = EdgeDefinition(
            edge_collection=collection_mapping.target_collection,
            from_collection=fk_a.foreign_table,
            to_collection=fk_b.foreign_table,
            from_fields=fk_a.columns,
            to_fields=fk_b.columns,
        )
        return cls(edge_def, table, key_separator=key_separator, join_mode=True)

    def _vertex_key_from_pk(self, row: Dict[str, Any]) -> str:
        if not self.source_table.primary_key:
            raise ValueError(f"Table '{self.source_table.name}' has no primary key; cannot build edge endpoint key")
        parts: list[str] = []
        for pk_col in self.source_table.primary_key:
            val = row.get(pk_col)
            if val is None:
                raise ValueError(f"Row missing PK value for column '{pk_col}': {row}")
            parts.append(str(val))
        return self.key_separator.join(parts)

    def transform_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if self.join_mode:
            return self._transform_join_row(row)
        return self._transform_fk_edge_row(row)

    def _transform_fk_edge_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        fk_parts: list[str] = []
        for ff in self.edge_def.from_fields:
            if ff not in row:
                logger.warning("edge_missing_from_field", field=ff, table=self.source_table.name)
                return None
            val = row[ff]
            if val is None:
                return None
            if isinstance(val, str) and val.strip() == "":
                return None
            fk_parts.append(str(val).strip())

        try:
            src_key = self._vertex_key_from_pk(row)
        except ValueError as e:
            logger.warning("edge_source_key_failed", error=str(e), table=self.source_table.name)
            return None

        to_key = self.key_separator.join(fk_parts)
        edge_key = f"{src_key}{self.key_separator}{to_key}"
        return {
            "_key": edge_key,
            "_from": f"{self.edge_def.from_collection}/{src_key}",
            "_to": f"{self.edge_def.to_collection}/{to_key}",
            "_label": self.edge_def.edge_collection,
        }

    def _transform_join_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        from_parts: list[str] = []
        for ff in self.edge_def.from_fields:
            if ff not in row:
                logger.warning("join_edge_missing_field", field=ff, table=self.source_table.name)
                return None
            v = row[ff]
            if v is None:
                return None
            if isinstance(v, str) and v.strip() == "":
                return None
            from_parts.append(str(v).strip())

        to_parts: list[str] = []
        for tf in self.edge_def.to_fields:
            if tf not in row:
                logger.warning("join_edge_missing_field", field=tf, table=self.source_table.name)
                return None
            v = row[tf]
            if v is None:
                return None
            if isinstance(v, str) and v.strip() == "":
                return None
            to_parts.append(str(v).strip())

        from_key = self.key_separator.join(from_parts)
        to_key = self.key_separator.join(to_parts)
        edge_key = f"{from_key}{self.key_separator}{to_key}"
        return {
            "_key": edge_key,
            "_from": f"{self.edge_def.from_collection}/{from_key}",
            "_to": f"{self.edge_def.to_collection}/{to_key}",
            "_label": self.edge_def.edge_collection,
        }

    def transform_rows(self, rows: Iterable[Dict[str, Any]]) -> Generator[Dict[str, Any], None, None]:
        for row in rows:
            out = self.transform_row(row)
            if out is not None:
                yield out
