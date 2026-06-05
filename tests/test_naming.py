from __future__ import annotations

import pytest

from r2g.naming import apply_naming_convention, convert_identifier, split_identifier
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    FieldExpression,
    ForeignKey,
    MappingConfig,
    NamingConvention,
    Schema,
    Table,
)


class TestSplitIdentifier:
    @pytest.mark.parametrize(
        "name,expected",
        [
            ("customer_id", ["customer", "id"]),
            ("CustomerOrder", ["Customer", "Order"]),
            ("customerOrder", ["customer", "Order"]),
            ("customer-to-invoice", ["customer", "to", "invoice"]),
            ("HTTPServer", ["HTTP", "Server"]),
            ("address_line2", ["address", "line2"]),
            ("  ", []),
        ],
    )
    def test_split(self, name, expected):
        assert split_identifier(name) == expected


class TestConvertIdentifier:
    @pytest.mark.parametrize(
        "name,style,expected",
        [
            ("customer_id", "snake", "customer_id"),
            ("customer_id", "camel", "customerId"),
            ("customer_id", "pascal", "CustomerId"),
            ("CustomerOrder", "snake", "customer_order"),
            ("CustomerOrder", "camel", "customerOrder"),
            ("customer_to_invoice", "pascal", "CustomerToInvoice"),
            ("customer_to_invoice", "camel", "customerToInvoice"),
            ("anything", "preserve", "anything"),
            ("", "pascal", ""),
        ],
    )
    def test_convert(self, name, style, expected):
        assert convert_identifier(name, style) == expected


@pytest.fixture
def schema():
    return Schema(
        tables={
            "customer": Table(
                name="customer",
                columns=[
                    Column(name="customer_id", data_type="integer", is_primary_key=True),
                    Column(name="first_name", data_type="text"),
                    Column(name="last_name", data_type="text"),
                ],
                primary_key=["customer_id"],
            ),
            "invoice": Table(
                name="invoice",
                columns=[
                    Column(name="invoice_id", data_type="integer", is_primary_key=True),
                    Column(name="customer_id", data_type="integer"),
                ],
                primary_key=["invoice_id"],
                foreign_keys=[
                    ForeignKey(column="customer_id", foreign_table="customer", foreign_column="customer_id"),
                ],
            ),
        }
    )


@pytest.fixture
def config():
    return MappingConfig(
        collections={
            "customer": CollectionMapping(source_table="customer", target_collection="customer"),
            "invoice": CollectionMapping(source_table="invoice", target_collection="invoice"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="invoice_to_customer",
                from_collection="invoice",
                to_collection="customer",
                from_field="customer_id",
                to_field="customer_id",
            )
        ],
    )


class TestApplyNamingConvention:
    def test_collections_pascal(self, config, schema):
        out = apply_naming_convention(config, NamingConvention(collections="pascal"), schema)
        assert out.collections["customer"].target_collection == "Customer"
        assert out.collections["invoice"].target_collection == "Invoice"

    def test_properties_camel_populates_field_mappings(self, config, schema):
        out = apply_naming_convention(config, NamingConvention(properties="camel"), schema)
        fm = out.collections["customer"].field_mappings
        assert fm["first_name"] == "firstName"
        assert fm["last_name"] == "lastName"
        assert fm["customer_id"] == "customerId"

    def test_edges_camel(self, config, schema):
        out = apply_naming_convention(config, NamingConvention(edges="camel"), schema)
        assert out.edges[0].edge_collection == "invoiceToCustomer"

    def test_edge_endpoints_left_as_source_keys(self, config, schema):
        """from/to_collection stay source-table keys so the pipeline can resolve them."""
        out = apply_naming_convention(
            config, NamingConvention(collections="pascal", edges="camel"), schema
        )
        assert out.edges[0].from_collection == "invoice"
        assert out.edges[0].to_collection == "customer"

    def test_preserve_is_noop(self, config, schema):
        out = apply_naming_convention(config, NamingConvention(), schema)
        assert out.collections["customer"].target_collection == "customer"
        assert out.collections["customer"].field_mappings == {}
        assert out.edges[0].edge_collection == "invoice_to_customer"

    def test_manual_renames_preserved(self, config, schema):
        config.collections["customer"].field_mappings = {"first_name": "givenName"}
        out = apply_naming_convention(config, NamingConvention(properties="camel"), schema)
        fm = out.collections["customer"].field_mappings
        assert fm["first_name"] == "givenName"  # not overwritten
        assert fm["last_name"] == "lastName"  # newly added

    def test_field_expression_target_recased(self, config, schema):
        config.collections["customer"].field_expressions = [
            FieldExpression(target="full_name", sources=["first_name", "last_name"], expression="CONCAT(@first_name,@last_name)")
        ]
        out = apply_naming_convention(config, NamingConvention(properties="camel"), schema)
        assert out.collections["customer"].field_expressions[0].target == "fullName"

    def test_system_fields_never_touched(self, config, schema):
        config.collections["customer"].field_expressions = [
            FieldExpression(target="_key", sources=["customer_id"])
        ]
        out = apply_naming_convention(config, NamingConvention(properties="pascal"), schema)
        assert out.collections["customer"].field_expressions[0].target == "_key"

    def test_records_convention(self, config, schema):
        out = apply_naming_convention(config, NamingConvention(collections="pascal"), schema)
        assert out.naming_convention is not None
        assert out.naming_convention.collections == "pascal"

    def test_original_config_unmodified(self, config, schema):
        apply_naming_convention(config, NamingConvention(collections="pascal"), schema)
        assert config.collections["customer"].target_collection == "customer"

    def test_reserved_source_columns_skipped(self, config):
        """A source column named like a system attribute is never remapped."""
        schema = Schema(tables={
            "weird": Table(
                name="weird",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="_key", data_type="text"),
                    Column(name="_from", data_type="text"),
                ],
                primary_key=["id"],
            ),
        })
        cfg = MappingConfig(collections={
            "weird": CollectionMapping(source_table="weird", target_collection="weird"),
        })
        out = apply_naming_convention(cfg, NamingConvention(properties="camel"), schema)
        fm = out.collections["weird"].field_mappings
        assert "_key" not in fm
        assert "_from" not in fm

    def test_no_schema_recases_existing_mappings_only(self, config):
        config.collections["customer"].field_mappings = {"first_name": "first_name"}
        out = apply_naming_convention(config, NamingConvention(properties="camel"), schema=None)
        assert out.collections["customer"].field_mappings == {"first_name": "firstName"}
