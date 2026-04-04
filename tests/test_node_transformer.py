from __future__ import annotations

import pytest

from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import CollectionMapping, Column, Table


def _simple_table(columns=None, primary_key=None):
    if columns is None:
        columns = [
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="name", data_type="text", is_nullable=False),
            Column(name="email", data_type="text", is_nullable=True),
        ]
    if primary_key is None:
        primary_key = ["id"]
    return Table(name="users", columns=columns, primary_key=primary_key, foreign_keys=[])


class TestBasicTransformWithoutMapping:
    def test_adds_key_and_preserves_fields(self):
        table = _simple_table()
        transformer = NodeTransformer(table)
        row = {"id": "1", "name": "Alice", "email": "a@b.com"}
        result = transformer.transform_row(row)

        assert result["_key"] == "1"
        assert result["id"] == "1"
        assert result["name"] == "Alice"
        assert result["email"] == "a@b.com"

    def test_returns_copy_not_original(self):
        table = _simple_table()
        transformer = NodeTransformer(table)
        row = {"id": "1", "name": "Alice", "email": "a@b.com"}
        result = transformer.transform_row(row)
        assert result is not row


class TestCompositePrimaryKey:
    def test_generates_composite_key_with_default_separator(self):
        table = Table(
            name="enrollment",
            columns=[
                Column(name="student_id", data_type="integer", is_primary_key=True),
                Column(name="course_id", data_type="integer", is_primary_key=True),
            ],
            primary_key=["student_id", "course_id"],
        )
        transformer = NodeTransformer(table)
        row = {"student_id": "1", "course_id": "2"}
        result = transformer.transform_row(row)

        assert result["_key"] == "1_2"

    def test_custom_separator(self):
        table = Table(
            name="enrollment",
            columns=[
                Column(name="a", data_type="integer", is_primary_key=True),
                Column(name="b", data_type="integer", is_primary_key=True),
            ],
            primary_key=["a", "b"],
        )
        transformer = NodeTransformer(table, key_separator="-")
        row = {"a": "10", "b": "20"}
        result = transformer.transform_row(row)

        assert result["_key"] == "10-20"


class TestTypeCoercion:
    def _make_transformer(self, col_type):
        table = Table(
            name="t",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="val", data_type=col_type, is_nullable=True),
            ],
            primary_key=["id"],
        )
        mapping = CollectionMapping(source_table="t", target_collection="t")
        return NodeTransformer(table, collection_mapping=mapping)

    def test_integer_coercion(self):
        t = self._make_transformer("integer")
        result = t.transform_row({"id": "1", "val": "42"})
        assert result["val"] == 42
        assert isinstance(result["val"], int)

    def test_float_coercion(self):
        t = self._make_transformer("numeric")
        result = t.transform_row({"id": "1", "val": "3.14"})
        assert result["val"] == pytest.approx(3.14)
        assert isinstance(result["val"], float)

    def test_boolean_coercion_true(self):
        t = self._make_transformer("boolean")
        result = t.transform_row({"id": "1", "val": "true"})
        assert result["val"] is True

    def test_boolean_coercion_false(self):
        t = self._make_transformer("boolean")
        result = t.transform_row({"id": "1", "val": "no"})
        assert result["val"] is False

    def test_string_coercion(self):
        t = self._make_transformer("text")
        result = t.transform_row({"id": "1", "val": 123})
        assert result["val"] == "123"
        assert isinstance(result["val"], str)


class TestFieldMappings:
    def test_renames_fields(self):
        table = _simple_table()
        mapping = CollectionMapping(
            source_table="users",
            target_collection="users",
            field_mappings={"name": "full_name", "email": "email_address"},
        )
        transformer = NodeTransformer(table, collection_mapping=mapping)
        row = {"id": "1", "name": "Alice", "email": "a@b.com"}
        result = transformer.transform_row(row)

        assert "full_name" in result
        assert result["full_name"] == "Alice"
        assert "email_address" in result
        assert result["email_address"] == "a@b.com"
        assert "name" not in result
        assert "email" not in result


class TestExcludeFields:
    def test_removes_excluded_fields(self):
        table = _simple_table()
        mapping = CollectionMapping(
            source_table="users",
            target_collection="users",
            exclude_fields=["email"],
        )
        transformer = NodeTransformer(table, collection_mapping=mapping)
        row = {"id": "1", "name": "Alice", "email": "a@b.com"}
        result = transformer.transform_row(row)

        assert "email" not in result
        assert result["name"] == "Alice"
        assert result["_key"] == "1"


class TestIncludeFields:
    def test_limits_to_included_fields(self):
        table = _simple_table()
        mapping = CollectionMapping(
            source_table="users",
            target_collection="users",
            include_fields=["id", "name"],
        )
        transformer = NodeTransformer(table, collection_mapping=mapping)
        row = {"id": "1", "name": "Alice", "email": "a@b.com"}
        result = transformer.transform_row(row)

        assert "email" not in result
        assert result["name"] == "Alice"
        assert result["_key"] == "1"


class TestNullableEmptyString:
    def test_empty_string_coerced_to_none(self):
        table = Table(
            name="t",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="note", data_type="text", is_nullable=True),
            ],
            primary_key=["id"],
        )
        mapping = CollectionMapping(source_table="t", target_collection="t")
        transformer = NodeTransformer(table, collection_mapping=mapping)
        result = transformer.transform_row({"id": "1", "note": ""})

        assert result["note"] is None

    def test_non_nullable_empty_string_stays(self):
        table = Table(
            name="t",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="note", data_type="text", is_nullable=False),
            ],
            primary_key=["id"],
        )
        mapping = CollectionMapping(source_table="t", target_collection="t")
        transformer = NodeTransformer(table, collection_mapping=mapping)
        result = transformer.transform_row({"id": "1", "note": ""})

        assert result["note"] == ""
