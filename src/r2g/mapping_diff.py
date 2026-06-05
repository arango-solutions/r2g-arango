from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from r2g.types import RESERVED_ATTRIBUTES, MappingConfig, Schema


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
    # Structured bind parameters for the executor (e.g. {"old_name", "new_name"}).
    # Preferred over parsing names out of ``reason``.
    params: dict[str, Any] = Field(default_factory=dict)


# In-place attribute rename: copy the old attribute to the new name and drop the
# old one in a single REPLACE. Bound with @old_name / @new_name / @@coll.
_RENAME_PROPERTY_AQL = (
    "FOR doc IN @@coll"
    " FILTER HAS(doc, @old_name)"
    " REPLACE doc WITH MERGE(UNSET(doc, @old_name), {@new_name: doc.@old_name})"
    " IN @@coll"
)


def _edges_referencing_source(source_table: str, edges: list) -> list[str]:
    """Edge-collection names whose endpoints reference a source table.

    Edges carry *source-table* keys in ``from_collection`` / ``to_collection``
    (this is what the pipeline and ``validate_config`` rely on), so renames of a
    collection's display name are detected against this stable identity.
    """
    result = []
    for e in edges:
        if e.from_collection == source_table or e.to_collection == source_table:
            if e.edge_collection not in result:
                result.append(e.edge_collection)
    return result


class ReloadPlan(BaseModel):
    changes: list[MappingChange] = Field(default_factory=list)
    actions: list[ReloadAction] = Field(default_factory=list)
    estimated_rows: int = 0
    estimated_time_seconds: float = 0.0


def _edge_by_name(edges: list) -> dict[str, Any]:
    return {e.edge_collection: e for e in edges}


def _action_key(action: ReloadAction) -> tuple:
    return (
        action.action_type,
        action.collection,
        tuple(sorted(action.params.items())),
    )


def _add_action(actions: list[ReloadAction], action: ReloadAction) -> None:
    key = _action_key(action)
    for existing in actions:
        if _action_key(existing) == key:
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
            for edge_name in _edges_referencing_source(key, old.edges):
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
            old_name = old_coll.target_collection
            new_name = new_coll.target_collection
            plan.changes.append(MappingChange(
                change_type="collection_renamed",
                collection=new_name,
                details={"old_name": old_name, "new_name": new_name},
            ))
            _add_action(plan.actions, ReloadAction(
                action_type="rename_collection",
                collection=old_name,
                reason=f"renamed to '{new_name}'",
                params={"old_name": old_name, "new_name": new_name},
            ))
            # Edges that touch this collection must have their _from/_to rebuilt
            # to point at the new name. Endpoints are derived from source FK data,
            # so we reload the affected edges rather than rewrite strings in place.
            affected = dict.fromkeys(
                _edges_referencing_source(key, old.edges)
                + _edges_referencing_source(key, new.edges)
            )
            for edge_name in affected:
                _add_action(plan.actions, ReloadAction(
                    action_type="reload_edge",
                    collection=edge_name,
                    reason=f"endpoints reference renamed collection '{old_name}' -> '{new_name}'",
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

        # A source column's property name in the DB is its mapped name, or the
        # column name itself when unmapped (pass-through). Compare those to find
        # the minimal in-place attribute rename for each changed column.
        for field_key in dict.fromkeys(list(old_fm) + list(new_fm)):
            old_prop = old_fm.get(field_key, field_key)
            new_prop = new_fm.get(field_key, field_key)
            if old_prop == new_prop:
                continue
            # Never rename ArangoDB system attributes in place.
            if old_prop in RESERVED_ATTRIBUTES or new_prop in RESERVED_ATTRIBUTES:
                continue

            if field_key not in old_fm:
                change_type = "field_mapping_added"
                details = {"field": field_key, "mapped_to": new_fm[field_key]}
            elif field_key not in new_fm:
                change_type = "field_mapping_removed"
                details = {"field": field_key, "was_mapped_to": old_fm[field_key]}
            else:
                change_type = "field_mapping_changed"
                details = {"field": field_key, "old": old_fm[field_key], "new": new_fm[field_key]}

            plan.changes.append(MappingChange(
                change_type=change_type,
                collection=target,
                details=details,
            ))
            _add_action(plan.actions, ReloadAction(
                action_type="aql_update",
                collection=target,
                reason=f"rename property '{old_prop}' to '{new_prop}'",
                aql_query=_RENAME_PROPERTY_AQL,
                params={"old_name": old_prop, "new_name": new_prop},
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


def _edge_identity(edge: Any) -> tuple:
    """Relationship identity of an edge, independent of its collection name."""
    return (
        edge.from_collection,
        edge.to_collection,
        tuple(edge.from_fields),
        tuple(edge.to_fields),
    )


def _detect_edge_changes(old: MappingConfig, new: MappingConfig, plan: ReloadPlan) -> None:
    old_edges = _edge_by_name(old.edges)
    new_edges = _edge_by_name(new.edges)

    # Detect pure renames first: same relationship, different edge-collection
    # name. These are in-place collection renames, not drop+reload.
    old_by_identity: dict[tuple, Any] = {}
    for e in old.edges:
        old_by_identity.setdefault(_edge_identity(e), e)
    renamed: dict[str, str] = {}  # old_edge_name -> new_edge_name
    for ne in new.edges:
        oe = old_by_identity.get(_edge_identity(ne))
        if oe is not None and oe.edge_collection != ne.edge_collection \
                and oe.edge_collection not in new_edges \
                and ne.edge_collection not in old_edges:
            renamed[oe.edge_collection] = ne.edge_collection

    for old_name, new_name in renamed.items():
        plan.changes.append(MappingChange(
            change_type="edge_renamed",
            edge=new_name,
            details={"old_name": old_name, "new_name": new_name},
        ))
        _add_action(plan.actions, ReloadAction(
            action_type="rename_collection",
            collection=old_name,
            reason=f"renamed to '{new_name}'",
            params={"old_name": old_name, "new_name": new_name},
        ))

    for name, edge in new_edges.items():
        if name in renamed.values():
            continue
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
        if name in renamed:
            continue
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

    # Any rename invalidates the named-graph edge definitions, which reference
    # collections by name; rebuild it once after the renames are applied.
    if any(c.change_type in ("collection_renamed", "edge_renamed") for c in plan.changes):
        _add_action(plan.actions, ReloadAction(
            action_type="rebuild_graph",
            collection="",
            reason="rebuild named graph after rename(s)",
        ))

    return plan
