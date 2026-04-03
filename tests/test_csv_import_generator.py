from __future__ import annotations

import os
import stat

import pytest

from r2g.config import ConfigManager
from r2g.generators.arangoimport import CsvImportGenerator
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
def ecommerce_schema() -> Schema:
    customers = Table(
        name="customers",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="name", data_type="text", is_nullable=False),
            Column(name="email", data_type="text", is_nullable=True),
            Column(name="is_premium", data_type="boolean", is_nullable=False),
            Column(name="balance", data_type="numeric", is_nullable=True),
        ],
        primary_key=["id"],
        foreign_keys=[],
    )
    orders = Table(
        name="orders",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="customer_id", data_type="integer", is_nullable=False),
            Column(name="total", data_type="numeric", is_nullable=False),
            Column(name="referrer_id", data_type="integer", is_nullable=True),
            Column(name="notes", data_type="text", is_nullable=True),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(column="customer_id", foreign_table="customers", foreign_column="id", constraint_name="fk_cust"),
            ForeignKey(column="referrer_id", foreign_table="customers", foreign_column="id", constraint_name="fk_ref"),
        ],
    )
    order_items = Table(
        name="order_items",
        columns=[
            Column(name="order_id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="product_id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="quantity", data_type="integer", is_nullable=False),
        ],
        primary_key=["order_id", "product_id"],
        foreign_keys=[
            ForeignKey(column="order_id", foreign_table="orders", foreign_column="id", constraint_name="fk_order"),
            ForeignKey(column="product_id", foreign_table="products", foreign_column="id", constraint_name="fk_prod"),
        ],
    )
    products = Table(
        name="products",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="name", data_type="text", is_nullable=False),
            Column(name="price", data_type="numeric", is_nullable=False),
        ],
        primary_key=["id"],
        foreign_keys=[],
    )
    return Schema(tables={
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
        "products": products,
    })


@pytest.fixture
def ecommerce_config() -> MappingConfig:
    return MappingConfig(
        collections={
            "customers": CollectionMapping(source_table="customers", target_collection="customers"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
            "products": CollectionMapping(source_table="products", target_collection="products"),
            "order_items": CollectionMapping(source_table="order_items", target_collection="order_items"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="orders_to_customers",
                from_collection="orders",
                to_collection="customers",
                from_field="customer_id",
                to_field="id",
            ),
            EdgeDefinition(
                edge_collection="orders_to_customers_referrer_id",
                from_collection="orders",
                to_collection="customers",
                from_field="referrer_id",
                to_field="id",
            ),
        ],
    )


@pytest.fixture
def generator(ecommerce_config, ecommerce_schema) -> CsvImportGenerator:
    return CsvImportGenerator(
        ecommerce_config,
        ecommerce_schema,
        endpoint="http://localhost:8529",
        database="test_db",
        username="root",
        password="secret",
        data_dir="./dumps",
    )


class TestInvalidOnDuplicate:
    def test_raises_value_error(self, ecommerce_config, ecommerce_schema):
        with pytest.raises(ValueError, match="on_duplicate"):
            CsvImportGenerator(ecommerce_config, ecommerce_schema, on_duplicate="bad")

    @pytest.mark.parametrize("valid", ["error", "update", "replace", "ignore"])
    def test_valid_values_accepted(self, ecommerce_config, ecommerce_schema, valid):
        gen = CsvImportGenerator(ecommerce_config, ecommerce_schema, on_duplicate=valid)
        assert gen.on_duplicate == valid


class TestDatatypeFlags:
    def test_skips_excluded_columns(self, generator):
        flags = generator._datatype_flags("customers", exclude_cols={"id"})
        joined = " ".join(flags)
        assert "id=" not in joined

    def test_includes_boolean_for_non_nullable(self, generator):
        flags = generator._datatype_flags("customers", exclude_cols={"id"})
        joined = " ".join(flags)
        assert "is_premium=boolean" in joined

    def test_skips_nullable_number(self, generator):
        flags = generator._datatype_flags("customers", exclude_cols={"id"})
        joined = " ".join(flags)
        assert "balance=" not in joined

    def test_returns_empty_for_unknown_table(self, generator):
        flags = generator._datatype_flags("nonexistent")
        assert flags == []

    def test_includes_non_nullable_number(self, generator):
        flags = generator._datatype_flags("orders", exclude_cols={"id", "customer_id", "referrer_id"})
        joined = " ".join(flags)
        assert "total=number" in joined

    def test_skips_nullable_referrer_id_number(self, generator):
        flags = generator._datatype_flags("orders", exclude_cols={"id"})
        joined = " ".join(flags)
        assert "referrer_id=" not in joined


class TestBuildDocCommand:
    def _normalize(self, cmd: str) -> str:
        return cmd.replace(" \\\n    ", " ")

    def test_uses_csv_type(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "--type csv" in cmd

    def test_translates_single_pk_to_key(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "--translate" in cmd
        assert "id=_key" in cmd

    def test_forces_pk_to_string(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "id=string" in cmd

    def test_composite_pk_uses_merge_attributes(self, generator):
        cmd = self._normalize(generator._build_doc_command("order_items", "order_items"))
        assert "--merge-attributes" in cmd
        assert "[order_id]_[product_id]" in cmd

    def test_composite_pk_forces_string(self, generator):
        cmd = self._normalize(generator._build_doc_command("order_items", "order_items"))
        assert "order_id=string" in cmd
        assert "product_id=string" in cmd

    def test_contains_overwrite(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "--overwrite" in cmd

    def test_uses_on_duplicate(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "--on-duplicate" in cmd
        assert "replace" in cmd

    def test_includes_server_env_vars(self, generator):
        cmd = self._normalize(generator._build_doc_command("customers", "customers"))
        assert "$ARANGO_ENDPOINT" in cmd
        assert "$ARANGO_DB" in cmd


class TestBuildEdgeCommand:
    def _normalize(self, cmd: str) -> str:
        return cmd.replace(" \\\n    ", " ")

    def test_uses_csv_type(self, generator):
        cmd = self._normalize(generator._build_edge_command("orders", "orders_to_customers", "orders", "customers", "customer_id"))
        assert "--type csv" in cmd

    def test_translates_pk_to_from(self, generator):
        cmd = self._normalize(generator._build_edge_command("orders", "orders_to_customers", "orders", "customers", "customer_id"))
        assert "id=_from" in cmd

    def test_translates_fk_to_to(self, generator):
        cmd = self._normalize(generator._build_edge_command("orders", "orders_to_customers", "orders", "customers", "customer_id"))
        assert "customer_id=_to" in cmd

    def test_from_collection_prefix(self, generator):
        cmd = self._normalize(generator._build_edge_command("orders", "orders_to_customers", "orders", "customers", "customer_id"))
        assert "--from-collection-prefix" in cmd
        assert "orders/" in cmd

    def test_to_collection_prefix(self, generator):
        cmd = self._normalize(generator._build_edge_command("orders", "orders_to_customers", "orders", "customers", "customer_id"))
        assert "--to-collection-prefix" in cmd
        assert "customers/" in cmd

    def test_removes_non_structural_columns(self, generator):
        cmd = self._normalize(generator._build_edge_command("orders", "orders_to_customers", "orders", "customers", "customer_id"))
        assert "--remove-attribute" in cmd
        assert "notes" in cmd

    def test_creates_edge_collection_type(self, generator):
        cmd = self._normalize(generator._build_edge_command("orders", "orders_to_customers", "orders", "customers", "customer_id"))
        assert "--create-collection-type edge" in cmd

    def test_forces_fk_to_string(self, generator):
        cmd = self._normalize(generator._build_edge_command("orders", "orders_to_customers", "orders", "customers", "customer_id"))
        assert "customer_id=string" in cmd


class TestBuildGraphCreationArangosh:
    def test_contains_graph_name(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "my_graph" in joined

    def test_uses_arangosh(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "arangosh" in joined

    def test_contains_edge_definitions(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "orders_to_customers" in joined

    def test_contains_relation_call(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "graph._relation(" in joined

    def test_contains_create_call(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "graph._create(" in joined

    def test_contains_drop_call(self, generator):
        lines = generator._build_graph_creation_arangosh("my_graph")
        joined = "\n".join(lines)
        assert "graph._drop(" in joined


class TestGenerateCsvScript:
    def test_shebang_present(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert content.startswith("#!/usr/bin/env bash")

    def test_set_pipefail(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "set -euo pipefail" in content

    def test_file_is_executable(self, generator, tmp_path):
        path = tmp_path / "import.sh"
        generator.generate_csv_script(str(path))
        mode = path.stat().st_mode
        assert mode & stat.S_IXUSR
        assert mode & stat.S_IXGRP
        assert mode & stat.S_IXOTH

    def test_file_written_to_disk(self, generator, tmp_path):
        path = tmp_path / "import.sh"
        generator.generate_csv_script(str(path))
        assert path.exists()
        content = path.read_text(encoding="utf-8")
        assert content.startswith("#!/usr/bin/env bash")

    def test_document_section(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "Document collections" in content
        assert "customers" in content
        assert "orders" in content

    def test_edge_section(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "Edge collections" in content
        assert "orders_to_customers" in content

    def test_no_graph_by_default(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "Named graph" not in content

    def test_graph_included_when_specified(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path, graph_name="my_graph")
        assert "Named graph" in content
        assert "my_graph" in content
        assert "arangosh" in content

    def test_env_defaults(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "ARANGO_ENDPOINT" in content
        assert "ARANGO_DB" in content
        assert "ARANGO_USER" in content
        assert "ARANGO_PASSWORD" in content

    def test_uses_csv_not_jsonl(self, generator, tmp_path):
        path = str(tmp_path / "import.sh")
        content = generator.generate_csv_script(path)
        assert "--type csv" in content
        assert "jsonl" not in content.lower().replace("# no intermediate jsonl", "").replace("no jsonl transformation", "")


class TestFromSampleSchema:
    def test_generate_from_auto_config(self, ecommerce_schema, tmp_path):
        config = ConfigManager.generate_default_config(ecommerce_schema)
        gen = CsvImportGenerator(config, ecommerce_schema)
        path = str(tmp_path / "import.sh")
        content = gen.generate_csv_script(path, graph_name="ecom_graph")
        assert "customers" in content
        assert "orders" in content
        assert "ecom_graph" in content
        assert "arangosh" in content
