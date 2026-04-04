from __future__ import annotations

import pytest
from pydantic import ValidationError

from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    ForeignKey,
    MappingConfig,
    Schema,
    Table,
)


class TestSchemaSerializationRoundTrip:
    def test_save_and_load_preserves_tables(self, sample_schema, tmp_path):
        path = str(tmp_path / "schema.json")
        sample_schema.save_to_file(path)
        loaded = Schema.load_from_file(path)

        assert set(loaded.tables.keys()) == {"users", "orders"}
        assert loaded.tables["users"].primary_key == ["id"]
        assert len(loaded.tables["users"].columns) == 3
        assert loaded.tables["orders"].foreign_keys[0].foreign_table == "users"

    def test_round_trip_column_attributes(self, sample_schema, tmp_path):
        path = str(tmp_path / "schema.json")
        sample_schema.save_to_file(path)
        loaded = Schema.load_from_file(path)

        email_col = next(c for c in loaded.tables["users"].columns if c.name == "email")
        assert email_col.is_nullable is True
        assert email_col.data_type == "text"

        id_col = next(c for c in loaded.tables["users"].columns if c.name == "id")
        assert id_col.is_primary_key is True
        assert id_col.is_nullable is False


class TestMappingConfigSerializationRoundTrip:
    def test_save_and_load_preserves_config(self, tmp_path):
        config = MappingConfig(
            source_schema="myschema",
            key_separator="-",
            collections={
                "users": CollectionMapping(
                    source_table="users",
                    target_collection="users",
                    field_mappings={"id": "user_id"},
                    exclude_fields=["secret"],
                ),
            },
            edges=[
                EdgeDefinition(
                    edge_collection="has_order",
                    from_collection="users",
                    to_collection="orders",
                    from_field="id",
                    to_field="user_id",
                ),
            ],
            type_overrides={"age": "string"},
        )
        path = str(tmp_path / "config.json")
        config.save_to_file(path)
        loaded = MappingConfig.load_from_file(path)

        assert loaded.source_schema == "myschema"
        assert loaded.key_separator == "-"
        assert loaded.collections["users"].field_mappings == {"id": "user_id"}
        assert loaded.collections["users"].exclude_fields == ["secret"]
        assert len(loaded.edges) == 1
        assert loaded.edges[0].edge_collection == "has_order"
        assert loaded.type_overrides == {"age": "string"}


class TestPydanticValidation:
    def test_column_missing_name_raises(self):
        with pytest.raises(ValidationError):
            Column(data_type="text")  # type: ignore[call-arg]

    def test_column_missing_data_type_raises(self):
        with pytest.raises(ValidationError):
            Column(name="x")  # type: ignore[call-arg]

    def test_table_missing_name_raises(self):
        with pytest.raises(ValidationError):
            Table(columns=[])  # type: ignore[call-arg]

    def test_table_missing_columns_raises(self):
        with pytest.raises(ValidationError):
            Table(name="t")  # type: ignore[call-arg]

    def test_foreign_key_missing_fields_raises(self):
        with pytest.raises(ValidationError):
            ForeignKey(column="x")  # type: ignore[call-arg]

    def test_edge_definition_missing_fields_raises(self):
        with pytest.raises(ValidationError):
            EdgeDefinition(edge_collection="e")  # type: ignore[call-arg]

    def test_collection_mapping_missing_required(self):
        with pytest.raises(ValidationError):
            CollectionMapping(target_collection="x")  # type: ignore[call-arg]


class TestDefaultValues:
    def test_column_defaults(self):
        col = Column(name="x", data_type="text")
        assert col.is_nullable is False
        assert col.is_primary_key is False

    def test_table_defaults(self):
        tbl = Table(name="t", columns=[])
        assert tbl.primary_key == []
        assert tbl.foreign_keys == []

    def test_foreign_key_constraint_name_default(self):
        fk = ForeignKey(column="c", foreign_table="t", foreign_column="id")
        assert fk.constraint_name is None

    def test_schema_empty_default(self):
        s = Schema()
        assert s.tables == {}

    def test_mapping_config_defaults(self):
        mc = MappingConfig()
        assert mc.source_schema == "public"
        assert mc.collections == {}
        assert mc.edges == []
        assert mc.type_overrides == {}
        assert mc.key_separator == "_"

    def test_collection_mapping_defaults(self):
        cm = CollectionMapping(source_table="t", target_collection="t")
        assert cm.collection_type == "document"
        assert cm.is_join_table is False
        assert cm.field_mappings == {}
        assert cm.exclude_fields == []
        assert cm.include_fields is None


class TestForeignKeyComposite:
    def test_singular_form_accepted(self):
        fk = ForeignKey(column="user_id", foreign_table="users", foreign_column="id")
        assert fk.columns == ["user_id"]
        assert fk.foreign_columns == ["id"]

    def test_plural_form_accepted(self):
        fk = ForeignKey(columns=["a", "b"], foreign_table="t", foreign_columns=["x", "y"])
        assert fk.columns == ["a", "b"]
        assert fk.foreign_columns == ["x", "y"]

    def test_backward_compat_properties(self):
        fk = ForeignKey(column="c", foreign_table="t", foreign_column="id")
        assert fk.column == "c"
        assert fk.foreign_column == "id"

    def test_is_composite_false_for_single(self):
        fk = ForeignKey(column="c", foreign_table="t", foreign_column="id")
        assert fk.is_composite is False

    def test_is_composite_true_for_multi(self):
        fk = ForeignKey(columns=["a", "b"], foreign_table="t", foreign_columns=["x", "y"])
        assert fk.is_composite is True

    def test_serialization_singular(self):
        fk = ForeignKey(column="c", foreign_table="t", foreign_column="id")
        d = fk.model_dump()
        assert "column" in d
        assert "columns" not in d
        assert d["column"] == "c"
        assert d["foreign_column"] == "id"

    def test_serialization_composite(self):
        fk = ForeignKey(columns=["a", "b"], foreign_table="t", foreign_columns=["x", "y"])
        d = fk.model_dump()
        assert "columns" in d
        assert "column" not in d
        assert d["columns"] == ["a", "b"]
        assert d["foreign_columns"] == ["x", "y"]

    def test_round_trip_via_schema_file(self, tmp_path):
        schema = Schema(tables={
            "shipments": Table(
                name="shipments",
                columns=[Column(name="id", data_type="integer", is_primary_key=True)],
                primary_key=["id"],
                foreign_keys=[
                    ForeignKey(
                        columns=["order_id", "product_id"],
                        foreign_table="order_items",
                        foreign_columns=["order_id", "product_id"],
                        constraint_name="fk_ship",
                    ),
                ],
            ),
        })
        path = str(tmp_path / "schema.json")
        schema.save_to_file(path)
        loaded = Schema.load_from_file(path)
        fk = loaded.tables["shipments"].foreign_keys[0]
        assert fk.columns == ["order_id", "product_id"]
        assert fk.foreign_columns == ["order_id", "product_id"]
        assert fk.is_composite is True


class TestEdgeDefinitionComposite:
    def test_singular_form_accepted(self):
        ed = EdgeDefinition(
            edge_collection="e", from_collection="a", to_collection="b",
            from_field="x", to_field="y",
        )
        assert ed.from_fields == ["x"]
        assert ed.to_fields == ["y"]

    def test_plural_form_accepted(self):
        ed = EdgeDefinition(
            edge_collection="e", from_collection="a", to_collection="b",
            from_fields=["x1", "x2"], to_fields=["y1", "y2"],
        )
        assert ed.from_fields == ["x1", "x2"]
        assert ed.to_fields == ["y1", "y2"]

    def test_backward_compat_properties(self):
        ed = EdgeDefinition(
            edge_collection="e", from_collection="a", to_collection="b",
            from_field="x", to_field="y",
        )
        assert ed.from_field == "x"
        assert ed.to_field == "y"

    def test_is_composite(self):
        single = EdgeDefinition(
            edge_collection="e", from_collection="a", to_collection="b",
            from_field="x", to_field="y",
        )
        composite = EdgeDefinition(
            edge_collection="e", from_collection="a", to_collection="b",
            from_fields=["x1", "x2"], to_fields=["y1", "y2"],
        )
        assert single.is_composite is False
        assert composite.is_composite is True

    def test_serialization_singular(self):
        ed = EdgeDefinition(
            edge_collection="e", from_collection="a", to_collection="b",
            from_field="x", to_field="y",
        )
        d = ed.model_dump()
        assert d["from_field"] == "x"
        assert d["to_field"] == "y"
        assert "from_fields" not in d
        assert "to_fields" not in d

    def test_serialization_composite(self):
        ed = EdgeDefinition(
            edge_collection="e", from_collection="a", to_collection="b",
            from_fields=["x1", "x2"], to_fields=["y1", "y2"],
        )
        d = ed.model_dump()
        assert d["from_fields"] == ["x1", "x2"]
        assert d["to_fields"] == ["y1", "y2"]
        assert "from_field" not in d
        assert "to_field" not in d
