from __future__ import annotations

import pytest

from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    ForeignKey,
    Schema,
    Table,
)
from r2g.transformers.edge_transformer import EdgeTransformer


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
