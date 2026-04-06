"""Migrate a mapping config when the source PostgreSQL schema evolves.

Given an existing MappingConfig and a new Schema, produces an updated
config that preserves user customizations while adapting to schema changes.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from r2g.config import _is_likely_join_table
from r2g.types import (
    CollectionMapping,
    EdgeDefinition,
    MappingConfig,
    Schema,
)


def _edge_fk_sig(edge: EdgeDefinition) -> tuple[str, tuple[str, ...], str, tuple[str, ...]]:
    """Canonical signature for matching an edge to a schema FK."""
    return (
        edge.from_collection,
        tuple(edge.from_fields),
        edge.to_collection,
        tuple(edge.to_fields),
    )


def _fk_to_edge_sig(
    table_name: str, fk: Any,
) -> tuple[str, tuple[str, ...], str, tuple[str, ...]]:
    return (
        table_name,
        tuple(fk.columns),
        fk.foreign_table,
        tuple(fk.foreign_columns),
    )


class MigrationReport:
    """Collects changes made during config migration for user review."""

    def __init__(self) -> None:
        self.added_collections: list[str] = []
        self.orphaned_collections: list[str] = []
        self.added_edges: list[str] = []
        self.removed_edges: list[str] = []
        self.cleaned_fields: list[str] = []

    @property
    def has_changes(self) -> bool:
        return bool(
            self.added_collections
            or self.orphaned_collections
            or self.added_edges
            or self.removed_edges
            or self.cleaned_fields
        )


def migrate_config(
    old_config: MappingConfig,
    new_schema: Schema,
    source_schema: str | None = None,
) -> tuple[MappingConfig, MigrationReport]:
    """Produce an updated config aligned with *new_schema*.

    Returns (new_config, report). The original old_config is not mutated.
    """
    report = MigrationReport()
    config = deepcopy(old_config)

    if source_schema is not None:
        config.source_schema = source_schema

    existing_tables = {cm.source_table for cm in config.collections.values()}
    schema_tables = set(new_schema.tables)

    for table_name in sorted(schema_tables - existing_tables):
        table = new_schema.tables[table_name]
        is_join = _is_likely_join_table(table)
        config.collections[table_name] = CollectionMapping(
            source_table=table_name,
            target_collection=table_name,
            collection_type="document",
            is_join_table=is_join,
        )
        report.added_collections.append(table_name)

    for key, cm in list(config.collections.items()):
        if cm.source_table not in schema_tables:
            report.orphaned_collections.append(key)

    for key, cm in config.collections.items():
        if cm.source_table not in schema_tables:
            continue
        new_cols = {c.name for c in new_schema.tables[cm.source_table].columns}

        cleaned_fm = {k: v for k, v in cm.field_mappings.items() if k in new_cols}
        if len(cleaned_fm) < len(cm.field_mappings):
            removed = set(cm.field_mappings) - set(cleaned_fm)
            for r in sorted(removed):
                report.cleaned_fields.append(
                    f"{key}: removed field_mapping '{r}' (column dropped)"
                )
            cm.field_mappings = cleaned_fm

        cleaned_ef = [f for f in cm.exclude_fields if f in new_cols]
        if len(cleaned_ef) < len(cm.exclude_fields):
            removed = set(cm.exclude_fields) - set(cleaned_ef)
            for r in sorted(removed):
                report.cleaned_fields.append(
                    f"{key}: removed exclude_field '{r}' (column dropped)"
                )
            cm.exclude_fields = cleaned_ef

        if cm.include_fields is not None:
            cleaned_if = [f for f in cm.include_fields if f in new_cols]
            if len(cleaned_if) < len(cm.include_fields):
                removed = set(cm.include_fields) - set(cleaned_if)
                for r in sorted(removed):
                    report.cleaned_fields.append(
                        f"{key}: removed include_field '{r}' (column dropped)"
                    )
                cm.include_fields = cleaned_if if cleaned_if else None

    existing_edge_sigs = {_edge_fk_sig(e) for e in config.edges}

    new_fk_sigs: set[tuple[str, tuple[str, ...], str, tuple[str, ...]]] = set()
    for table_name, table in new_schema.tables.items():
        for fk in table.foreign_keys:
            new_fk_sigs.add(_fk_to_edge_sig(table_name, fk))

    kept_edges = []
    for e in config.edges:
        if _edge_fk_sig(e) in new_fk_sigs:
            kept_edges.append(e)
        else:
            sig = _edge_fk_sig(e)
            report.removed_edges.append(
                f"{e.edge_collection} ({sig[0]} -> {sig[2]})"
            )
    config.edges = kept_edges

    edge_names_used = {e.edge_collection for e in config.edges}
    for table_name, table in sorted(new_schema.tables.items()):
        for fk in table.foreign_keys:
            sig = _fk_to_edge_sig(table_name, fk)
            if sig not in existing_edge_sigs:
                base = f"{table_name}_to_{fk.foreign_table}"
                edge_name = base
                if edge_name in edge_names_used:
                    suffix = "_".join(fk.columns)
                    edge_name = f"{base}_{suffix}"
                edge_names_used.add(edge_name)
                config.edges.append(
                    EdgeDefinition(
                        edge_collection=edge_name,
                        from_collection=table_name,
                        to_collection=fk.foreign_table,
                        from_fields=fk.columns,
                        to_fields=fk.foreign_columns,
                    )
                )
                report.added_edges.append(edge_name)

    to_clean = set()
    for key in config.type_overrides:
        parts = key.split(".", 1)
        if len(parts) == 2:
            tbl, col = parts
            if tbl not in schema_tables:
                to_clean.add(key)
            elif col not in {c.name for c in new_schema.tables[tbl].columns}:
                to_clean.add(key)
    for key in sorted(to_clean):
        del config.type_overrides[key]
        report.cleaned_fields.append(
            f"removed type_override '{key}' (table/column dropped)"
        )

    return config, report
