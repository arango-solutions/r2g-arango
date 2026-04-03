from __future__ import annotations

import pytest

from r2g.config import ConfigManager, DEFAULT_TYPE_MAP, _is_likely_join_table, pg_type_to_json_type
from r2g.types import Column, ForeignKey, MappingConfig, Schema, Table


class TestPgTypeToJsonType:
    @pytest.mark.parametrize(
        "pg_type, expected",
        [
            ("integer", "integer"),
            ("bigint", "integer"),
            ("smallint", "integer"),
            ("serial", "integer"),
            ("bigserial", "integer"),
            ("numeric", "float"),
            ("decimal", "float"),
            ("real", "float"),
            ("double precision", "float"),
            ("boolean", "boolean"),
            ("json", "object"),
            ("jsonb", "object"),
        ],
    )
    def test_known_types(self, pg_type, expected):
        assert pg_type_to_json_type(pg_type) == expected

    @pytest.mark.parametrize(
        "pg_type",
        ["text", "varchar", "character varying", "uuid", "timestamp", "date"],
    )
    def test_unknown_types_fallback_to_string(self, pg_type):
        assert pg_type_to_json_type(pg_type) == "string"

    @pytest.mark.parametrize(
        "pg_type",
        ["integer[]", "text[]", "ARRAY", "boolean[]"],
    )
    def test_array_types(self, pg_type):
        assert pg_type_to_json_type(pg_type) == "array"

    def test_type_with_precision_stripped(self):
        assert pg_type_to_json_type("numeric(10,2)") == "float"

    def test_case_insensitive(self):
        assert pg_type_to_json_type("INTEGER") == "integer"
        assert pg_type_to_json_type("Boolean") == "boolean"

    def test_whitespace_stripped(self):
        assert pg_type_to_json_type("  integer  ") == "integer"


class TestGenerateDefaultConfig:
    def test_produces_document_collections(self, sample_schema):
        config = ConfigManager.generate_default_config(sample_schema)
        assert "users" in config.collections
        assert "orders" in config.collections
        assert len(config.collections) == 2
        assert config.collections["users"].collection_type == "document"
        assert config.collections["orders"].collection_type == "document"

    def test_produces_edge_definitions(self, sample_schema):
        config = ConfigManager.generate_default_config(sample_schema)
        assert len(config.edges) == 1
        edge = config.edges[0]
        assert edge.edge_collection == "orders_to_users"
        assert edge.from_collection == "orders"
        assert edge.to_collection == "users"
        assert edge.from_field == "user_id"
        assert edge.to_field == "id"

    def test_collection_mapping_source_table(self, sample_schema):
        config = ConfigManager.generate_default_config(sample_schema)
        assert config.collections["users"].source_table == "users"
        assert config.collections["users"].target_collection == "users"

    def test_default_schema_is_public(self, sample_schema):
        config = ConfigManager.generate_default_config(sample_schema)
        assert config.source_schema == "public"


class TestSaveAndLoadConfig:
    def test_yaml_round_trip(self, sample_schema, tmp_path):
        config = ConfigManager.generate_default_config(sample_schema)
        path = tmp_path / "mapping.yaml"
        ConfigManager.save_config(config, path)
        loaded = ConfigManager.load_config(path)

        assert loaded.source_schema == config.source_schema
        assert set(loaded.collections.keys()) == set(config.collections.keys())
        assert len(loaded.edges) == len(config.edges)
        assert loaded.edges[0].edge_collection == config.edges[0].edge_collection

    def test_load_empty_file_returns_default(self, tmp_path):
        path = tmp_path / "empty.yaml"
        path.write_text("", encoding="utf-8")
        loaded = ConfigManager.load_config(path)

        assert isinstance(loaded, MappingConfig)
        assert loaded.source_schema == "public"
        assert loaded.collections == {}
        assert loaded.edges == []

    def test_load_invalid_yaml_raises(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text("- just a list item\n", encoding="utf-8")
        with pytest.raises(ValueError, match="YAML mapping"):
            ConfigManager.load_config(path)

    def test_save_creates_parent_dirs(self, tmp_path):
        config = MappingConfig()
        path = tmp_path / "a" / "b" / "config.yaml"
        ConfigManager.save_config(config, path)
        assert path.exists()


class TestJoinTableAutoDetection:
    def _make_table(self, name, cols, pks, fks):
        columns = [
            Column(name=c[0], data_type=c[1], is_nullable=c[2], is_primary_key=c[0] in pks)
            for c in cols
        ]
        foreign_keys = [
            ForeignKey(column=fk[0], foreign_table=fk[1], foreign_column=fk[2], constraint_name=f"fk_{fk[0]}")
            for fk in fks
        ]
        return Table(name=name, columns=columns, primary_key=pks, foreign_keys=foreign_keys)

    def test_pure_join_table_detected(self):
        table = self._make_table(
            "post_tags",
            [("post_id", "integer", False), ("tag_id", "integer", False)],
            ["post_id", "tag_id"],
            [("post_id", "posts", "id"), ("tag_id", "tags", "id")],
        )
        assert _is_likely_join_table(table) is True

    def test_join_table_with_quantity(self):
        table = self._make_table(
            "order_items",
            [("order_id", "integer", False), ("product_id", "integer", False),
             ("quantity", "integer", False)],
            ["order_id", "product_id"],
            [("order_id", "orders", "id"), ("product_id", "products", "id")],
        )
        assert _is_likely_join_table(table) is True

    def test_join_table_with_created_at(self):
        table = self._make_table(
            "user_roles",
            [("user_id", "integer", False), ("role_id", "integer", False),
             ("created_at", "timestamp without time zone", True)],
            ["user_id", "role_id"],
            [("user_id", "users", "id"), ("role_id", "roles", "id")],
        )
        assert _is_likely_join_table(table) is True

    def test_regular_table_not_detected(self):
        table = self._make_table(
            "orders",
            [("id", "integer", False), ("customer_id", "integer", False),
             ("total", "numeric", False), ("notes", "text", True)],
            ["id"],
            [("customer_id", "customers", "id")],
        )
        assert _is_likely_join_table(table) is False

    def test_table_with_one_fk_not_detected(self):
        table = self._make_table(
            "comments",
            [("id", "integer", False), ("post_id", "integer", False),
             ("body", "text", False)],
            ["id"],
            [("post_id", "posts", "id")],
        )
        assert _is_likely_join_table(table) is False

    def test_table_with_extra_data_column_not_detected(self):
        table = self._make_table(
            "enrollments",
            [("student_id", "integer", False), ("course_id", "integer", False),
             ("grade", "text", True)],
            ["student_id", "course_id"],
            [("student_id", "students", "id"), ("course_id", "courses", "id")],
        )
        assert _is_likely_join_table(table) is False

    def test_generate_default_config_flags_join_table(self):
        post_tags = self._make_table(
            "post_tags",
            [("post_id", "integer", False), ("tag_id", "integer", False)],
            ["post_id", "tag_id"],
            [("post_id", "posts", "id"), ("tag_id", "tags", "id")],
        )
        posts = self._make_table(
            "posts",
            [("id", "integer", False), ("title", "text", False)],
            ["id"],
            [],
        )
        tags = self._make_table(
            "tags",
            [("id", "integer", False), ("label", "text", False)],
            ["id"],
            [],
        )
        schema = Schema(tables={"posts": posts, "tags": tags, "post_tags": post_tags})
        config = ConfigManager.generate_default_config(schema)
        assert config.collections["post_tags"].is_join_table is True
        assert config.collections["posts"].is_join_table is False
        assert config.collections["tags"].is_join_table is False
