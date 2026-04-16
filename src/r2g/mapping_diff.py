from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from r2g.types import MappingConfig, Schema


class MappingChange(BaseModel):
    change_type: str
    collection: str | None = None
    edge: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class ReloadAction(BaseModel):
    action_type: str
    collection: str
    reason: str
    sql_query: str | None = None
    aql_query: str | None = None


class ReloadPlan(BaseModel):
    changes: list[MappingChange] = Field(default_factory=list)
    actions: list[ReloadAction] = Field(default_factory=list)
    estimated_rows: int = 0
    estimated_time_seconds: float = 0.0


def _edge_by_name(edges: list) -> dict[str, Any]:
    return {e.edge_collection: e for e in edges}


def _collections_referencing(collection_name: str, edges: list) -> list[str]:
    result = []
    for e in edges:
        if e.from_collection == collection_name or e.to_collection == collection_name:
            if e.edge_collection not in result:
                result.append(e.edge_collection)
    return result


def _add_action(actions: list[ReloadAction], action: ReloadAction) -> None:
    for existing in actions:
        if existing.action_type == action.action_type and existing.collection == action.collection:
            return
    actions.append(action)


def _detect_key_separator(old: MappingConfig, new: MappingConfig, plan: ReloadPlan) -> bool:
    if old.key_separator == new.key_separator:
        return False
    plan.changes.append(MappingChange(
        change_type="key_separator_changed",
        details={"old": old.key_separator, "new": new.key_separator},
    ))
    for key, coll in new.collections.items():
        _add_action(plan.actions, ReloadAction(
            action_type="reload_collection",
            collection=coll.target_collection,
            reason=f"key_separator changed from '{old.key_separator}' to '{new.key_separator}'",
        ))
    return True


def _detect_collection_added(old: MappingConfig, new: MappingConfig, plan: ReloadPlan) -> None:
    for key, coll in new.collections.items():
        if key not in old.collections:
            plan.changes.append(MappingChange(
                change_type="collection_added",
                collection=coll.target_collection,
                details={"source_table": coll.source_table},
            ))
            _add_action(plan.actions, ReloadAction(
                action_type="reload_collection",
                collection=coll.target_collection,
                reason=f"new collection from table '{coll.source_table}'",
            ))


def _detect_collection_removed(old: MappingConfig, new: MappingConfig, plan: ReloadPlan) -> None:
    for key, coll in old.collections.items():
        if key not in new.collections:
            plan.changes.append(MappingChange(
                change_type="collection_removed",
                collection=coll.target_collection,
                details={"source_table": coll.source_table},
            ))
            _add_action(plan.actions, ReloadAction(
                action_type="drop_collection",
                collection=coll.target_collection,
                reason="collection removed from mapping",
            ))
            for edge_name in _collections_referencing(coll.target_collection, old.edges):
                _add_action(plan.actions, ReloadAction(
                    action_type="drop_edge",
                    collection=edge_name,
                    reason=f"references dropped collection '{coll.target_collection}'",
                ))


def _detect_collection_renamed(old: MappingConfig, new: MappingConfig, plan: ReloadPlan) -> None:
    for key in old.collections:
        if key not in new.collections:
            continue
        old_coll = old.collections[key]
        new_coll = new.collections[key]
        if old_coll.source_table == new_coll.source_table and old_coll.target_collection != new_coll.target_collection:
            plan.changes.append(MappingChange(
                change_type="collection_renamed",
                collection=new_coll.target_collection,
                details={"old_name": old_coll.target_collection, "new_name": new_coll.target_collection},
            ))
            _add_action(plan.actions, ReloadAction(
                action_type="rename_collection",
                collection=old_coll.target_collection,
                reason=f"renamed to '{new_coll.target_collection}'",
            ))
            affected = (
                _collections_referencing(old_coll.target_collection, old.edges)
                + _collections_referencing(new_coll.target_collection, new.edges)
            )
            for edge_name in dict.fromkeys(affected):
                old_name = old_coll.target_collection
                new_name = new_coll.target_collection
                _add_action(plan.actions, ReloadAction(
                    action_type="aql_update",
                    collection=edge_name,
                    reason=f"update _from/_to references from '{old_name}' to '{new_name}'",
                    aql_query=(
                        "FOR doc IN @@edge_collection"
                        " UPDATE doc WITH"
                        " {_from: SUBSTITUTE(doc._from, @old_prefix, @new_prefix),"
                        " _to: SUBSTITUTE(doc._to, @old_prefix, @new_prefix)}"
                        " IN @@edge_collection"
                    ),
                ))


def _detect_field_mapping_changes(old: MappingConfig, new: MappingConfig, plan: ReloadPlan) -> None:
    for key in old.collections:
        if key not in new.collections:
            continue
        old_coll = old.collections[key]
        new_coll = new.collections[key]
        target = new_coll.target_collection

        old_fm = old_coll.field_mappings
        new_fm = new_coll.field_mappings

        for field_key in new_fm:
            if field_key not in old_fm:
                plan.changes.append(MappingChange(
                    change_type="field_mapping_added",
                    collection=target,
                    details={"field": field_key, "mapped_to": new_fm[field_key]},
                ))
                _add_action(plan.actions, ReloadAction(
                    action_type="aql_update",
                    collection=target,
                    reason=f"rename field '{field_key}' to '{new_fm[field_key]}'",
                    aql_query=(
                        "FOR doc IN @@coll"
                        " UPDATE doc WITH {@new_name: doc.@old_name} IN @@coll"
                    ),
                ))

        for field_key in old_fm:
            if field_key not in new_fm:
                plan.changes.append(MappingChange(
                    change_type="field_mapping_removed",
                    collection=target,
                    details={"field": field_key, "was_mapped_to": old_fm[field_key]},
                ))
                _add_action(plan.actions, ReloadAction(
                    action_type="aql_update",
                    collection=target,
                    reason=f"remove field mapping for '{field_key}'",
                    aql_query=(
                        "FOR doc IN @@coll"
                        " LET d = UNSET(doc, @field_name)"
                        " REPLACE doc WITH d IN @@coll"
                    ),
                ))


def _detect_exclude_fields_changed(old: MappingConfig, new: MappingConfig, plan: ReloadPlan) -> None:
    for key in old.collections:
        if key not in new.collections:
            continue
        old_coll = old.collections[key]
        new_coll = new.collections[key]
        if sorted(old_coll.exclude_fields) != sorted(new_coll.exclude_fields):
            plan.changes.append(MappingChange(
                change_type="exclude_fields_changed",
                collection=new_coll.target_collection,
                details={"old": old_coll.exclude_fields, "new": new_coll.exclude_fields},
            ))
            _add_action(plan.actions, ReloadAction(
                action_type="reload_collection",
                collection=new_coll.target_collection,
                reason="exclude_fields changed",
            ))


def _detect_include_fields_changed(old: MappingConfig, new: MappingConfig, plan: ReloadPlan) -> None:
    for key in old.collections:
        if key not in new.collections:
            continue
        old_coll = old.collections[key]
        new_coll = new.collections[key]
        old_inc = sorted(old_coll.include_fields) if old_coll.include_fields is not None else None
        new_inc = sorted(new_coll.include_fields) if new_coll.include_fields is not None else None
        if old_inc != new_inc:
            plan.changes.append(MappingChange(
                change_type="include_fields_changed",
                collection=new_coll.target_collection,
                details={"old": old_coll.include_fields, "new": new_coll.include_fields},
            ))
            _add_action(plan.actions, ReloadAction(
                action_type="reload_collection",
                collection=new_coll.target_collection,
                reason="include_fields changed",
            ))


def _detect_edge_changes(old: MappingConfig, new: MappingConfig, plan: ReloadPlan) -> None:
    old_edges = _edge_by_name(old.edges)
    new_edges = _edge_by_name(new.edges)

    for name, edge in new_edges.items():
        if name not in old_edges:
            plan.changes.append(MappingChange(
                change_type="edge_added",
                edge=name,
                details={
                    "from_collection": edge.from_collection,
                    "to_collection": edge.to_collection,
                },
            ))
            _add_action(plan.actions, ReloadAction(
                action_type="reload_edge",
                collection=name,
                reason="new edge definition",
            ))

    for name, edge in old_edges.items():
        if name not in new_edges:
            plan.changes.append(MappingChange(
                change_type="edge_removed",
                edge=name,
                details={
                    "from_collection": edge.from_collection,
                    "to_collection": edge.to_collection,
                },
            ))
            _add_action(plan.actions, ReloadAction(
                action_type="drop_edge",
                collection=name,
                reason="edge removed from mapping",
            ))

    for name in old_edges:
        if name not in new_edges:
            continue
        oe = old_edges[name]
        ne = new_edges[name]
        if (
            oe.from_collection != ne.from_collection
            or oe.to_collection != ne.to_collection
            or oe.from_fields != ne.from_fields
            or oe.to_fields != ne.to_fields
        ):
            plan.changes.append(MappingChange(
                change_type="edge_modified",
                edge=name,
                details={"old_from": oe.from_collection, "new_from": ne.from_collection},
            ))
            _add_action(plan.actions, ReloadAction(
                action_type="drop_edge",
                collection=name,
                reason="edge definition changed",
            ))
            _add_action(plan.actions, ReloadAction(
                action_type="reload_edge",
                collection=name,
                reason="edge definition changed",
            ))


def _detect_type_override_changes(
    old: MappingConfig, new: MappingConfig, plan: ReloadPlan, schema: Schema
) -> None:
    all_keys = set(old.type_overrides) | set(new.type_overrides)
    affected_tables: set[str] = set()

    for key in all_keys:
        old_val = old.type_overrides.get(key)
        new_val = new.type_overrides.get(key)
        if old_val == new_val:
            continue

        if old_val is None:
            change_type = "type_override_added"
        elif new_val is None:
            change_type = "type_override_removed"
        else:
            change_type = "type_override_changed"

        plan.changes.append(MappingChange(
            change_type=change_type,
            details={"key": key, "old_value": old_val, "new_value": new_val},
        ))

        table_name = key.split(".")[0] if "." in key else key
        affected_tables.add(table_name)

    for table_name in affected_tables:
        for coll in new.collections.values():
            if coll.source_table == table_name:
                _add_action(plan.actions, ReloadAction(
                    action_type="reload_collection",
                    collection=coll.target_collection,
                    reason=f"type override changed for table '{table_name}'",
                ))


def diff_mappings(old: MappingConfig, new: MappingConfig, schema: Schema) -> ReloadPlan:
    """Compare two mapping configs and produce a minimal reload plan."""
    plan = ReloadPlan()

    if _detect_key_separator(old, new, plan):
        return plan

    _detect_collection_added(old, new, plan)
    _detect_collection_removed(old, new, plan)
    _detect_collection_renamed(old, new, plan)
    _detect_field_mapping_changes(old, new, plan)
    _detect_exclude_fields_changed(old, new, plan)
    _detect_include_fields_changed(old, new, plan)
    _detect_edge_changes(old, new, plan)
    _detect_type_override_changes(old, new, plan, schema)

    return plan
