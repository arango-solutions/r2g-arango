from __future__ import annotations

import pytest

from r2g.transformers.edge_transformer import EdgeTransformer
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    ForeignKey,
    Schema,
    Table,
)


def _orders_table():
    return Table(
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


def _edge_def():
    return EdgeDefinition(
        edge_collection="orders_to_users",
        from_collection="orders",
        to_collection="users",
        from_field="user_id",
        to_field="id",
    )


class TestFKEdge:
    def test_creates_correct_from_to_key_label(self):
        transformer = EdgeTransformer(_edge_def(), _orders_table())
        row = {"id": "10", "user_id": "3", "total": "99.99"}
        result = transformer.transform_row(row)

        assert result is not None
        assert result["_from"] == "orders/10"
        assert result["_to"] == "users/3"
        assert result["_key"] == "10_3"
        assert result["_label"] == "orders_to_users"


class TestEndpointResolution:
    def test_from_to_name_override_target_collections(self):
        """When collections are renamed, _from/_to use the resolved target names."""
        transformer = EdgeTransformer(
            _edge_def(), _orders_table(), from_name="Order", to_name="User"
        )
        row = {"id": "10", "user_id": "3", "total": "99.99"}
        result = transformer.transform_row(row)

        assert result is not None
        assert result["_from"] == "Order/10"
        assert result["_to"] == "User/3"
        # The edge collection label is unaffected by endpoint resolution.
        assert result["_label"] == "orders_to_users"

    def test_defaults_to_edge_def_collections(self):
        transformer = EdgeTransformer(_edge_def(), _orders_table())
        result = transformer.transform_row({"id": "1", "user_id": "2"})
        assert result["_from"] == "orders/1"
        assert result["_to"] == "users/2"


class TestNullFK:
    def test_null_fk_returns_none(self):
        transformer = EdgeTransformer(_edge_def(), _orders_table())
        row = {"id": "10", "user_id": None, "total": "99.99"}
        result = transformer.transform_row(row)

        assert result is None

    def test_empty_string_fk_returns_none(self):
        transformer = EdgeTransformer(_edge_def(), _orders_table())
        row = {"id": "10", "user_id": "", "total": "99.99"}
        result = transformer.transform_row(row)

        assert result is None

    def test_whitespace_fk_returns_none(self):
        transformer = EdgeTransformer(_edge_def(), _orders_table())
        row = {"id": "10", "user_id": "   ", "total": "99.99"}
        result = transformer.transform_row(row)

        assert result is None


class TestTransformRows:
    def test_yields_only_non_none_results(self):
        transformer = EdgeTransformer(_edge_def(), _orders_table())
        rows = [
            {"id": "1", "user_id": "10", "total": "1.00"},
            {"id": "2", "user_id": None, "total": "2.00"},
            {"id": "3", "user_id": "20", "total": "3.00"},
            {"id": "4", "user_id": "", "total": "4.00"},
        ]
        results = list(transformer.transform_rows(rows))

        assert len(results) == 2
        assert results[0]["_from"] == "orders/1"
        assert results[1]["_from"] == "orders/3"


class TestForJoinTable:
    def _join_setup(self):
        join_table = Table(
            name="student_courses",
            columns=[
                Column(name="student_id", data_type="integer"),
                Column(name="course_id", data_type="integer"),
            ],
            primary_key=["student_id", "course_id"],
            foreign_keys=[
                ForeignKey(column="student_id", foreign_table="students", foreign_column="id"),
                ForeignKey(column="course_id", foreign_table="courses", foreign_column="id"),
            ],
        )
        mapping = CollectionMapping(
            source_table="student_courses",
            target_collection="enrolled_in",
            collection_type="edge",
            is_join_table=True,
        )
        schema = Schema(
            tables={
                "students": Table(name="students", columns=[], primary_key=["id"]),
                "courses": Table(name="courses", columns=[], primary_key=["id"]),
                "student_courses": join_table,
            }
        )
        return join_table, mapping, schema

    def test_join_table_creates_edge(self):
        join_table, mapping, schema = self._join_setup()
        transformer = EdgeTransformer.for_join_table(join_table, mapping, schema)
        row = {"student_id": "5", "course_id": "10"}
        result = transformer.transform_row(row)

        assert result is not None
        assert result["_label"] == "enrolled_in"
        assert "_from" in result
        assert "_to" in result
        assert "_key" in result

    def test_join_table_from_to_collections(self):
        join_table, mapping, schema = self._join_setup()
        transformer = EdgeTransformer.for_join_table(join_table, mapping, schema)
        row = {"student_id": "5", "course_id": "10"}
        result = transformer.transform_row(row)

        from_prefix = result["_from"].split("/")[0]
        to_prefix = result["_to"].split("/")[0]
        assert {from_prefix, to_prefix} == {"courses", "students"}

    def test_wrong_number_of_fks_raises(self):
        bad_table = Table(
            name="bad",
            columns=[Column(name="a", data_type="integer")],
            primary_key=["a"],
            foreign_keys=[
                ForeignKey(column="a", foreign_table="x", foreign_column="id"),
            ],
        )
        mapping = CollectionMapping(
            source_table="bad",
            target_collection="bad_edge",
            collection_type="edge",
            is_join_table=True,
        )
        schema = Schema(tables={"x": Table(name="x", columns=[], primary_key=["id"])})

        with pytest.raises(ValueError, match="exactly 2 foreign keys"):
            EdgeTransformer.for_join_table(bad_table, mapping, schema)


class TestCompositeFKEdge:
    def _shipments_table(self):
        return Table(
            name="shipments",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="order_id", data_type="integer"),
                Column(name="product_id", data_type="integer"),
                Column(name="warehouse", data_type="text"),
            ],
            primary_key=["id"],
            foreign_keys=[
                ForeignKey(
                    columns=["order_id", "product_id"],
                    foreign_table="order_items",
                    foreign_columns=["order_id", "product_id"],
                    constraint_name="fk_ship_items",
                ),
            ],
        )

    def _composite_edge_def(self):
        return EdgeDefinition(
            edge_collection="shipments_to_order_items",
            from_collection="shipments",
            to_collection="order_items",
            from_fields=["order_id", "product_id"],
            to_fields=["order_id", "product_id"],
        )

    def test_composite_fk_edge_to_key(self):
        transformer = EdgeTransformer(self._composite_edge_def(), self._shipments_table())
        row = {"id": 1, "order_id": 3, "product_id": 5, "warehouse": "West"}
        result = transformer.transform_row(row)
        assert result is not None
        assert result["_to"] == "order_items/3_5"
        assert result["_from"] == "shipments/1"
        assert result["_key"] == "1_3_5"

    def test_composite_null_first_fk_returns_none(self):
        transformer = EdgeTransformer(self._composite_edge_def(), self._shipments_table())
        row = {"id": 1, "order_id": None, "product_id": 5, "warehouse": "West"}
        result = transformer.transform_row(row)
        assert result is None

    def test_composite_null_second_fk_returns_none(self):
        transformer = EdgeTransformer(self._composite_edge_def(), self._shipments_table())
        row = {"id": 1, "order_id": 3, "product_id": None, "warehouse": "West"}
        result = transformer.transform_row(row)
        assert result is None

    def test_composite_missing_fk_field_returns_none(self):
        transformer = EdgeTransformer(self._composite_edge_def(), self._shipments_table())
        row = {"id": 1, "order_id": 3, "warehouse": "West"}
        result = transformer.transform_row(row)
        assert result is None
