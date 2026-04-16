from __future__ import annotations

import pytest

from r2g.mapping_diff import diff_mappings
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    ForeignKey,
    MappingConfig,
    Schema,
    Table,
)


@pytest.fixture
def minimal_schema() -> Schema:
    users = Table(
        name="users",
        columns=[
            Column(name="id", data_type="integer", is_primary_key=True),
            Column(name="name", data_type="text"),
            Column(name="email", data_type="text", is_nullable=True),
        ],
        primary_key=["id"],
    )
    orders = Table(
        name="orders",
        columns=[
            Column(name="id", data_type="integer", is_primary_key=True),
            Column(name="user_id", data_type="integer"),
            Column(name="total", data_type="numeric", is_nullable=True),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(column="user_id", foreign_table="users", foreign_column="id"),
        ],
    )
    return Schema(tables={"users": users, "orders": orders})


def _base_config(**overrides) -> MappingConfig:
    defaults = dict(
        collections={
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="user_orders",
                from_collection="users",
                to_collection="orders",
                from_field="id",
                to_field="user_id",
            ),
        ],
    )
    defaults.update(overrides)
    return MappingConfig(**defaults)


class TestKeyChangeTypes:
    def test_key_separator_changed_reloads_all(self, minimal_schema):
        old = _base_config(key_separator="_")
        new = _base_config(key_separator="-")
        plan = diff_mappings(old, new, minimal_schema)

        assert len(plan.changes) == 1
        assert plan.changes[0].change_type == "key_separator_changed"
        assert plan.changes[0].details["old"] == "_"
        assert plan.changes[0].details["new"] == "-"

        reload_actions = [a for a in plan.actions if a.action_type == "reload_collection"]
        reloaded = {a.collection for a in reload_actions}
        assert "users" in reloaded
        assert "orders" in reloaded

    def test_key_separator_changed_takes_priority(self, minimal_schema):
        old = _base_config(key_separator="_")
        new_collections = {
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
            "products": CollectionMapping(source_table="products", target_collection="products"),
        }
        new = _base_config(key_separator="-", collections=new_collections)
        plan = diff_mappings(old, new, minimal_schema)

        change_types = {c.change_type for c in plan.changes}
        assert "key_separator_changed" in change_types
        assert "collection_added" not in change_types


class TestCollectionChanges:
    def test_collection_added(self, minimal_schema):
        old = _base_config()
        new_collections = {
            **old.collections,
            "products": CollectionMapping(source_table="products", target_collection="products"),
        }
        new = _base_config(collections=new_collections)
        plan = diff_mappings(old, new, minimal_schema)

        added = [c for c in plan.changes if c.change_type == "collection_added"]
        assert len(added) == 1
        assert added[0].collection == "products"

        reload = [a for a in plan.actions if a.action_type == "reload_collection" and a.collection == "products"]
        assert len(reload) == 1

    def test_collection_removed(self, minimal_schema):
        old = _base_config()
        new_collections = {"users": old.collections["users"]}
        new = _base_config(collections=new_collections, edges=[])
        plan = diff_mappings(old, new, minimal_schema)

        removed = [c for c in plan.changes if c.change_type == "collection_removed"]
        assert len(removed) == 1
        assert removed[0].collection == "orders"

        drop = [a for a in plan.actions if a.action_type == "drop_collection"]
        assert len(drop) == 1
        assert drop[0].collection == "orders"

        drop_edges = [a for a in plan.actions if a.action_type == "drop_edge"]
        assert any(a.collection == "user_orders" for a in drop_edges)

    def test_collection_renamed(self, minimal_schema):
        old = _base_config()
        new_collections = {
            "users": CollectionMapping(source_table="users", target_collection="app_users"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        new_edges = [
            EdgeDefinition(
                edge_collection="user_orders",
                from_collection="app_users",
                to_collection="orders",
                from_field="id",
                to_field="user_id",
            ),
        ]
        new = _base_config(collections=new_collections, edges=new_edges)
        plan = diff_mappings(old, new, minimal_schema)

        renamed = [c for c in plan.changes if c.change_type == "collection_renamed"]
        assert len(renamed) == 1
        assert renamed[0].details["old_name"] == "users"
        assert renamed[0].details["new_name"] == "app_users"

        rename_actions = [a for a in plan.actions if a.action_type == "rename_collection"]
        assert len(rename_actions) == 1

        aql_actions = [a for a in plan.actions if a.action_type == "aql_update" and a.collection == "user_orders"]
        assert len(aql_actions) == 1
        assert "SUBSTITUTE" in aql_actions[0].aql_query

    def test_exclude_fields_changed(self, minimal_schema):
        old_collections = {
            "users": CollectionMapping(source_table="users", target_collection="users", exclude_fields=["email"]),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        new_collections = {
            "users": CollectionMapping(source_table="users", target_collection="users", exclude_fields=[]),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        old = _base_config(collections=old_collections)
        new = _base_config(collections=new_collections)
        plan = diff_mappings(old, new, minimal_schema)

        changes = [c for c in plan.changes if c.change_type == "exclude_fields_changed"]
        assert len(changes) == 1
        assert changes[0].collection == "users"

        reload = [a for a in plan.actions if a.action_type == "reload_collection" and a.collection == "users"]
        assert len(reload) == 1

    def test_include_fields_changed(self, minimal_schema):
        old_collections = {
            "users": CollectionMapping(
                source_table="users", target_collection="users", include_fields=["id", "name"]
            ),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        new_collections = {
            "users": CollectionMapping(
                source_table="users", target_collection="users", include_fields=["id", "name", "email"]
            ),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        old = _base_config(collections=old_collections)
        new = _base_config(collections=new_collections)
        plan = diff_mappings(old, new, minimal_schema)

        changes = [c for c in plan.changes if c.change_type == "include_fields_changed"]
        assert len(changes) == 1
        assert changes[0].collection == "users"

        reload = [a for a in plan.actions if a.action_type == "reload_collection" and a.collection == "users"]
        assert len(reload) == 1

    def test_include_fields_none_to_list(self, minimal_schema):
        old_collections = {
            "users": CollectionMapping(source_table="users", target_collection="users", include_fields=None),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        new_collections = {
            "users": CollectionMapping(source_table="users", target_collection="users", include_fields=["id"]),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        old = _base_config(collections=old_collections)
        new = _base_config(collections=new_collections)
        plan = diff_mappings(old, new, minimal_schema)

        changes = [c for c in plan.changes if c.change_type == "include_fields_changed"]
        assert len(changes) == 1


class TestFieldMappingChanges:
    def test_field_mapping_added(self, minimal_schema):
        old_collections = {
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        new_collections = {
            "users": CollectionMapping(
                source_table="users", target_collection="users", field_mappings={"email": "email_address"}
            ),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        old = _base_config(collections=old_collections)
        new = _base_config(collections=new_collections)
        plan = diff_mappings(old, new, minimal_schema)

        added = [c for c in plan.changes if c.change_type == "field_mapping_added"]
        assert len(added) == 1
        assert added[0].collection == "users"
        assert added[0].details["field"] == "email"
        assert added[0].details["mapped_to"] == "email_address"

        aql = [a for a in plan.actions if a.action_type == "aql_update" and a.collection == "users"]
        assert len(aql) == 1
        assert "@new_name" in aql[0].aql_query

    def test_field_mapping_removed(self, minimal_schema):
        old_collections = {
            "users": CollectionMapping(
                source_table="users", target_collection="users", field_mappings={"email": "email_address"}
            ),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        new_collections = {
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        old = _base_config(collections=old_collections)
        new = _base_config(collections=new_collections)
        plan = diff_mappings(old, new, minimal_schema)

        removed = [c for c in plan.changes if c.change_type == "field_mapping_removed"]
        assert len(removed) == 1
        assert removed[0].collection == "users"
        assert removed[0].details["field"] == "email"

        aql = [a for a in plan.actions if a.action_type == "aql_update" and a.collection == "users"]
        assert len(aql) == 1
        assert "UNSET" in aql[0].aql_query


class TestEdgeChanges:
    def test_edge_added(self, minimal_schema):
        old = _base_config(edges=[])
        new = _base_config()
        plan = diff_mappings(old, new, minimal_schema)

        added = [c for c in plan.changes if c.change_type == "edge_added"]
        assert len(added) == 1
        assert added[0].edge == "user_orders"

        reload = [a for a in plan.actions if a.action_type == "reload_edge"]
        assert len(reload) == 1
        assert reload[0].collection == "user_orders"

    def test_edge_removed(self, minimal_schema):
        old = _base_config()
        new = _base_config(edges=[])
        plan = diff_mappings(old, new, minimal_schema)

        removed = [c for c in plan.changes if c.change_type == "edge_removed"]
        assert len(removed) == 1
        assert removed[0].edge == "user_orders"

        drop = [a for a in plan.actions if a.action_type == "drop_edge"]
        assert len(drop) == 1
        assert drop[0].collection == "user_orders"

    def test_edge_modified(self, minimal_schema):
        old = _base_config()
        new_edges = [
            EdgeDefinition(
                edge_collection="user_orders",
                from_collection="orders",
                to_collection="users",
                from_field="user_id",
                to_field="id",
            ),
        ]
        new = _base_config(edges=new_edges)
        plan = diff_mappings(old, new, minimal_schema)

        modified = [c for c in plan.changes if c.change_type == "edge_modified"]
        assert len(modified) == 1
        assert modified[0].edge == "user_orders"

        drop = [a for a in plan.actions if a.action_type == "drop_edge" and a.collection == "user_orders"]
        assert len(drop) == 1
        reload = [a for a in plan.actions if a.action_type == "reload_edge" and a.collection == "user_orders"]
        assert len(reload) == 1


class TestTypeOverrideChanges:
    def test_type_override_added(self, minimal_schema):
        old = _base_config(type_overrides={})
        new = _base_config(type_overrides={"users.email": "string"})
        plan = diff_mappings(old, new, minimal_schema)

        changes = [c for c in plan.changes if c.change_type == "type_override_added"]
        assert len(changes) == 1
        assert changes[0].details["key"] == "users.email"

        reload = [a for a in plan.actions if a.action_type == "reload_collection" and a.collection == "users"]
        assert len(reload) == 1

    def test_type_override_removed(self, minimal_schema):
        old = _base_config(type_overrides={"users.email": "string"})
        new = _base_config(type_overrides={})
        plan = diff_mappings(old, new, minimal_schema)

        changes = [c for c in plan.changes if c.change_type == "type_override_removed"]
        assert len(changes) == 1
        assert changes[0].details["key"] == "users.email"

        reload = [a for a in plan.actions if a.action_type == "reload_collection" and a.collection == "users"]
        assert len(reload) == 1

    def test_type_override_value_changed(self, minimal_schema):
        old = _base_config(type_overrides={"users.email": "string"})
        new = _base_config(type_overrides={"users.email": "text"})
        plan = diff_mappings(old, new, minimal_schema)

        changes = [c for c in plan.changes if c.change_type == "type_override_changed"]
        assert len(changes) == 1
        assert changes[0].details["old_value"] == "string"
        assert changes[0].details["new_value"] == "text"

        reload = [a for a in plan.actions if a.action_type == "reload_collection" and a.collection == "users"]
        assert len(reload) == 1


class TestNoChanges:
    def test_identical_configs_produce_empty_plan(self, minimal_schema):
        config = _base_config()
        plan = diff_mappings(config, config.model_copy(deep=True), minimal_schema)

        assert len(plan.changes) == 0
        assert len(plan.actions) == 0
        assert plan.estimated_rows == 0
        assert plan.estimated_time_seconds == 0.0


class TestMultipleChanges:
    def test_combined_collection_and_edge_changes(self, minimal_schema):
        old = _base_config()
        new_collections = {
            "users": CollectionMapping(
                source_table="users", target_collection="users", exclude_fields=["email"]
            ),
            "products": CollectionMapping(source_table="products", target_collection="products"),
        }
        new_edges = [
            EdgeDefinition(
                edge_collection="user_products",
                from_collection="users",
                to_collection="products",
                from_field="id",
                to_field="user_id",
            ),
        ]
        new = _base_config(collections=new_collections, edges=new_edges)
        plan = diff_mappings(old, new, minimal_schema)

        change_types = {c.change_type for c in plan.changes}
        assert "collection_added" in change_types
        assert "collection_removed" in change_types
        assert "exclude_fields_changed" in change_types
        assert "edge_added" in change_types
        assert "edge_removed" in change_types

        action_types = {a.action_type for a in plan.actions}
        assert "reload_collection" in action_types
        assert "drop_collection" in action_types
        assert "reload_edge" in action_types
        assert "drop_edge" in action_types

    def test_actions_are_deduplicated(self, minimal_schema):
        old_collections = {
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        new_collections = {
            "users": CollectionMapping(
                source_table="users",
                target_collection="users",
                exclude_fields=["email"],
                include_fields=["id", "name"],
            ),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        }
        old = _base_config(collections=old_collections)
        new = _base_config(collections=new_collections)
        plan = diff_mappings(old, new, minimal_schema)

        reload_users = [
            a for a in plan.actions if a.action_type == "reload_collection" and a.collection == "users"
        ]
        assert len(reload_users) == 1
