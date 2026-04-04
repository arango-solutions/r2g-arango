from __future__ import annotations

import stat

import pytest

from r2g.config import ConfigManager
from r2g.generators.arangoimport import ArangoImportGenerator
from r2g.types import CollectionMapping, EdgeDefinition, MappingConfig


@pytest.fixture
def simple_config():
    return MappingConfig(
        collections={
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="orders_to_users",
                from_collection="orders",
                to_collection="users",
                from_field="user_id",
                to_field="id",
            ),
        ],
    )


@pytest.fixture
def generator(simple_config):
    return ArangoImportGenerator(simple_config)


class TestGenerateDocumentCommands:
    def test_produces_commands_for_each_document_collection(self, generator):
        commands = generator.generate_document_commands()
        assert len(commands) == 2

    def test_commands_contain_collection_names(self, generator):
        commands = generator.generate_document_commands()
        joined = "\n".join(commands)
        assert "users" in joined
        assert "orders" in joined

    def test_commands_use_document_type(self, generator):
        commands = generator.generate_document_commands()
        for cmd in commands:
            assert "--create-collection-type document" in cmd

    def test_commands_include_arangoimport(self, generator):
        commands = generator.generate_document_commands()
        for cmd in commands:
            assert cmd.startswith("arangoimport")


class TestGenerateEdgeCommands:
    def test_produces_commands_for_each_edge(self, generator):
        commands = generator.generate_edge_commands()
        assert len(commands) == 1

    def test_commands_contain_edge_type(self, generator):
        commands = generator.generate_edge_commands()
        for cmd in commands:
            assert "--create-collection-type edge" in cmd

    def test_commands_contain_edge_collection_name(self, generator):
        commands = generator.generate_edge_commands()
        assert "orders_to_users" in commands[0]


class TestGenerateScript:
    def test_shebang_present(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_script(path)
        assert content.startswith("#!/usr/bin/env bash")

    def test_set_pipefail_present(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_script(path)
        assert "set -euo pipefail" in content

    def test_file_is_executable(self, generator, tmp_path):
        path = tmp_path / "import.sh"
        generator.generate_script(str(path))
        mode = path.stat().st_mode
        assert mode & stat.S_IXUSR
        assert mode & stat.S_IXGRP
        assert mode & stat.S_IXOTH

    def test_script_contains_document_and_edge_sections(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_script(path)
        assert "users" in content
        assert "orders" in content
        assert "orders_to_users" in content

    def test_script_written_to_disk(self, generator, tmp_path):
        path = tmp_path / "import.sh"
        generator.generate_script(str(path))
        assert path.exists()
        disk_content = path.read_text(encoding="utf-8")
        assert disk_content.startswith("#!/usr/bin/env bash")


class TestGenerateCreateGraphAql:
    def test_contains_graph_name(self, generator):
        aql = generator.generate_create_graph_aql("my_graph")
        assert "my_graph" in aql

    def test_contains_edge_definitions(self, generator):
        aql = generator.generate_create_graph_aql()
        assert "orders_to_users" in aql
        assert "orders" in aql
        assert "users" in aql

    def test_contains_relation_call(self, generator):
        aql = generator.generate_create_graph_aql()
        assert "graph._relation(" in aql

    def test_contains_create_call(self, generator):
        aql = generator.generate_create_graph_aql()
        assert "graph._create(" in aql

    def test_default_graph_name(self, generator):
        aql = generator.generate_create_graph_aql()
        assert "r2g_graph" in aql


class TestInvalidOnDuplicate:
    def test_raises_value_error(self, simple_config):
        with pytest.raises(ValueError, match="on_duplicate"):
            ArangoImportGenerator(simple_config, on_duplicate="bad_value")

    @pytest.mark.parametrize("valid", ["error", "update", "replace", "ignore"])
    def test_valid_values_accepted(self, simple_config, valid):
        gen = ArangoImportGenerator(simple_config, on_duplicate=valid)
        assert gen.on_duplicate == valid


class TestFromSampleSchema:
    def test_generate_from_schema(self, sample_schema, tmp_path):
        config = ConfigManager.generate_default_config(sample_schema)
        gen = ArangoImportGenerator(config)

        doc_cmds = gen.generate_document_commands()
        edge_cmds = gen.generate_edge_commands()
        assert len(doc_cmds) == 2
        assert len(edge_cmds) == 1

        path = str(tmp_path / "import.sh")
        content = gen.generate_script(path)
        assert "orders_to_users" in content
