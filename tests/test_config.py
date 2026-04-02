from __future__ import annotations

import pytest

from r2g.config import ConfigManager, DEFAULT_TYPE_MAP, pg_type_to_json_type
from r2g.types import MappingConfig


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
