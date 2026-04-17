from __future__ import annotations

import pytest

from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import CollectionMapping, Column, FieldExpression, Table


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


class TestFieldExpressions:
    def _people_table(self):
        return Table(
            name="people",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True, is_nullable=False),
                Column(name="first_name", data_type="text", is_nullable=False),
                Column(name="last_name", data_type="text", is_nullable=True),
                Column(name="age", data_type="integer", is_nullable=True),
            ],
            primary_key=["id"],
        )

    def test_concat_expression_produces_target(self):
        table = self._people_table()
        mapping = CollectionMapping(
            source_table="people",
            target_collection="people",
            field_expressions=[
                FieldExpression(
                    target="full_name",
                    sources=["first_name", "last_name"],
                    expression='CONCAT(@first_name, " ", @last_name)',
                )
            ],
        )
        transformer = NodeTransformer(table, collection_mapping=mapping)
        result = transformer.transform_row(
            {"id": 1, "first_name": "Ada", "last_name": "Lovelace", "age": 36}
        )

        assert result["full_name"] == "Ada Lovelace"
        assert result["first_name"] == "Ada"
        assert result["age"] == 36

    def test_expression_overrides_pass_through_for_same_target(self):
        table = self._people_table()
        mapping = CollectionMapping(
            source_table="people",
            target_collection="people",
            field_expressions=[
                FieldExpression(
                    target="first_name",
                    sources=["first_name"],
                    expression="UPPER(@first_name)",
                )
            ],
        )
        transformer = NodeTransformer(table, collection_mapping=mapping)
        result = transformer.transform_row(
            {"id": 1, "first_name": "ada", "last_name": "Lovelace", "age": 36}
        )

        assert result["first_name"] == "ADA"

    def test_expression_respects_field_mappings_for_target_name(self):
        table = self._people_table()
        mapping = CollectionMapping(
            source_table="people",
            target_collection="people",
            field_mappings={"first_name": "given_name"},
            field_expressions=[
                FieldExpression(
                    target="given_name",
                    sources=["first_name"],
                    expression="UPPER(@first_name)",
                )
            ],
        )
        transformer = NodeTransformer(table, collection_mapping=mapping)
        result = transformer.transform_row(
            {"id": 1, "first_name": "ada", "last_name": "L", "age": 1}
        )

        assert result["given_name"] == "ADA"
        assert "first_name" not in result

    def test_null_coalesce_fallback(self):
        table = self._people_table()
        mapping = CollectionMapping(
            source_table="people",
            target_collection="people",
            field_expressions=[
                FieldExpression(
                    target="display_last",
                    sources=["last_name"],
                    expression='@last_name ?? "unknown"',
                )
            ],
        )
        transformer = NodeTransformer(table, collection_mapping=mapping)
        row = {"id": 1, "first_name": "Ada", "last_name": None, "age": None}
        result = transformer.transform_row(row)

        assert result["display_last"] == "unknown"

    def test_identity_expression_passes_value_through(self):
        table = self._people_table()
        mapping = CollectionMapping(
            source_table="people",
            target_collection="people",
            field_expressions=[
                FieldExpression(target="age", sources=["age"], expression="")
            ],
        )
        transformer = NodeTransformer(table, collection_mapping=mapping)
        result = transformer.transform_row(
            {"id": 1, "first_name": "Ada", "last_name": "L", "age": "42"}
        )

        assert result["age"] == 42

    def test_uncompilable_expression_falls_back_to_source_pass_through(self):
        table = self._people_table()
        mapping = CollectionMapping(
            source_table="people",
            target_collection="people",
            field_expressions=[
                FieldExpression(
                    target="age_note",
                    sources=["age"],
                    expression="UNKNOWN_FUNC(@age)",
                )
            ],
        )
        transformer = NodeTransformer(table, collection_mapping=mapping)
        result = transformer.transform_row(
            {"id": 1, "first_name": "Ada", "last_name": "L", "age": "42"}
        )

        assert result["age_note"] == 42

    def test_non_aql_engine_falls_back_to_identity(self):
        table = self._people_table()
        mapping = CollectionMapping(
            source_table="people",
            target_collection="people",
            field_expressions=[
                FieldExpression(
                    target="first_name",
                    sources=["first_name"],
                    expression="first_name.upper()",
                    engine="python",
                )
            ],
        )
        transformer = NodeTransformer(table, collection_mapping=mapping)
        result = transformer.transform_row(
            {"id": 1, "first_name": "ada", "last_name": "L", "age": 1}
        )

        assert result["first_name"] == "ada"
