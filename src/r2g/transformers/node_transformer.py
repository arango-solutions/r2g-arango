from __future__ import annotations

import json
from typing import Any, Dict, Optional

from r2g.config import pg_type_to_json_type
from r2g.log import get_logger
from r2g.types import CollectionMapping, Column, Table

logger = get_logger(__name__)


class NodeTransformer:
    def __init__(
        self,
        table_def: Table,
        collection_mapping: Optional[CollectionMapping] = None,
        key_separator: str = "_",
        type_overrides: Optional[Dict[str, str]] = None,
    ) -> None:
        self.table_def = table_def
        self._mapping = collection_mapping
        self.key_separator = key_separator
        self._type_overrides = type_overrides or {}

    def _json_type_for_column(self, column: Column) -> str:
        if column.name in self._type_overrides:
            return self._type_overrides[column.name]
        return pg_type_to_json_type(column.data_type)

    def _coerce_value(self, value: Any, column: Column) -> Any:
        json_type = self._json_type_for_column(column)
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "" and column.is_nullable:
            return None

        if json_type == "integer":
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            return int(str(value).strip())

        if json_type == "float":
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
            return float(str(value).strip())

        if json_type == "boolean":
            if isinstance(value, str):
                return value.lower() in ("true", "1", "t", "yes")
            return bool(value)

        if json_type == "object":
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    logger.warning("json_decode_failed", column=column.name)
                    return value
            return value

        if json_type == "array":
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    logger.warning("json_decode_failed", column=column.name)
                    return value
            return value

        return str(value)

    def _generate_key(self, row: Dict[str, Any]) -> Optional[str]:
        if not self.table_def.primary_key:
            return None
        pk_values: list[str] = []
        for pk_col in self.table_def.primary_key:
            val = row.get(pk_col)
            if val is None:
                raise ValueError(f"Row missing PK value for column '{pk_col}': {row}")
            pk_values.append(str(val))
        return self.key_separator.join(pk_values)

    def transform_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        key = self._generate_key(row)

        if self._mapping is None:
            doc = row.copy()
            if key:
                doc["_key"] = key
            return doc

        col_by_name: Dict[str, Column] = {c.name: c for c in self.table_def.columns}
        known = set(col_by_name.keys())
        names = set(row.keys()) & known
        if self._mapping.include_fields is not None:
            names &= set(self._mapping.include_fields)
        names -= set(self._mapping.exclude_fields)

        ordered_names = [k for k in row if k in names]
        doc: Dict[str, Any] = {}
        for src_name in ordered_names:
            raw = row.get(src_name)
            column = col_by_name[src_name]
            try:
                coerced = self._coerce_value(raw, column)
            except (TypeError, ValueError) as e:
                logger.warning("coerce_failed", column=src_name, error=str(e))
                coerced = raw
            tgt_name = self._mapping.field_mappings.get(src_name, src_name)
            doc[tgt_name] = coerced

        if key:
            doc["_key"] = key
        return doc
