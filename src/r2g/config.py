from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Set

import yaml

from r2g.types import EdgeDefinition, CollectionMapping, MappingConfig, Schema, Table


DEFAULT_TYPE_MAP: Dict[str, str] = {
    "integer": "integer",
    "bigint": "integer",
    "smallint": "integer",
    "serial": "integer",
    "bigserial": "integer",
    "numeric": "float",
    "decimal": "float",
    "real": "float",
    "double precision": "float",
    "boolean": "boolean",
    "json": "object",
    "jsonb": "object",
}


def _base_pg_type_name(pg_type: str) -> str:
    t = pg_type.strip().lower()
    if "(" in t:
        t = t.split("(", 1)[0].strip()
    return t


def _is_array_pg_type(pg_type: str) -> bool:
    s = pg_type.strip().lower()
    return "[]" in s or s.startswith("array") or s.endswith("[]")


def pg_type_to_json_type(pg_type: str) -> str:
    if _is_array_pg_type(pg_type):
        return "array"
    base = _base_pg_type_name(pg_type)
    if base in DEFAULT_TYPE_MAP:
        return DEFAULT_TYPE_MAP[base]
    return "string"


def _is_likely_join_table(table: Table) -> bool:
    """Heuristic: a join table has exactly 2 FKs and no non-FK, non-PK data columns
    (or only typical junction metadata like quantity, created_at, etc.)."""
    if len(table.foreign_keys) != 2:
        return False
    fk_cols = {fk.column for fk in table.foreign_keys}
    pk_cols = set(table.primary_key)
    structural = fk_cols | pk_cols
    data_cols = [c for c in table.columns if c.name not in structural]
    if not data_cols:
        return True
    _JUNCTION_META = {"quantity", "qty", "count", "sort_order", "position", "rank",
                      "created_at", "updated_at", "created", "updated"}
    return all(c.name.lower() in _JUNCTION_META for c in data_cols)


class ConfigManager:
    """Load, save, and synthesize table-to-graph mapping configuration."""

    @staticmethod
    def generate_default_config(schema: Schema) -> MappingConfig:
        collections: Dict[str, CollectionMapping] = {}
        edges: list[EdgeDefinition] = []
        edge_collection_names: Set[str] = set()

        for table_name, table in schema.tables.items():
            is_join = _is_likely_join_table(table)
            collections[table_name] = CollectionMapping(
                source_table=table_name,
                target_collection=table_name,
                collection_type="document",
                is_join_table=is_join,
            )

        for table_name, table in schema.tables.items():
            for fk in table.foreign_keys:
                base = f"{table_name}_to_{fk.foreign_table}"
                edge_name = base
                if edge_name in edge_collection_names:
                    edge_name = f"{base}_{fk.column}"
                edge_collection_names.add(edge_name)
                edges.append(
                    EdgeDefinition(
                        edge_collection=edge_name,
                        from_collection=table_name,
                        to_collection=fk.foreign_table,
                        from_field=fk.column,
                        to_field=fk.foreign_column,
                    )
                )

        return MappingConfig(
            source_schema="public",
            collections=collections,
            edges=edges,
        )

    @staticmethod
    def load_config(path: str | Path) -> MappingConfig:
        p = Path(path)
        with p.open("r", encoding="utf-8") as f:
            data: Any = yaml.safe_load(f)
        if data is None:
            return MappingConfig()
        if not isinstance(data, dict):
            raise ValueError(f"Mapping config must be a YAML mapping, got {type(data).__name__}")
        return MappingConfig.model_validate(data)

    @staticmethod
    def save_config(config: MappingConfig, path: str | Path) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = config.model_dump(mode="python")
        with p.open("w", encoding="utf-8") as f:
            yaml.safe_dump(
                payload,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )
