from __future__ import annotations

from r2g.config import ConfigManager
from r2g.connectors.postgres import annotate_partition_metadata
from r2g.types import Column, ForeignKey, Schema, Table


def _payment_partition(name: str, *, with_fks: bool) -> Table:
    fks = (
        [
            ForeignKey(columns=["customer_id"], foreign_table="customer", foreign_columns=["customer_id"]),
            ForeignKey(columns=["rental_id"], foreign_table="rental", foreign_columns=["rental_id"]),
            ForeignKey(columns=["staff_id"], foreign_table="staff", foreign_columns=["staff_id"]),
        ]
        if with_fks
        else []
    )
    return Table(
        name=name,
        columns=[
            Column(name="payment_id", data_type="integer", is_primary_key=True),
            Column(name="customer_id", data_type="integer"),
            Column(name="rental_id", data_type="integer"),
            Column(name="staff_id", data_type="integer"),
            Column(name="payment_date", data_type="timestamp", is_primary_key=True),
        ],
        primary_key=["payment_date", "payment_id"],
        foreign_keys=fks,
    )


def _pagila_like_schema() -> Schema:
    # Parent + 3 partitions: p06 has declared FKs, p07 is missing them (mirrors
    # the real Pagila database), and customer is an ordinary referenced table.
    tables = {
        "payment": _payment_partition("payment", with_fks=False),
        "payment_p2022_06": _payment_partition("payment_p2022_06", with_fks=True),
        "payment_p2022_07": _payment_partition("payment_p2022_07", with_fks=False),
        "customer": Table(
            name="customer",
            columns=[Column(name="customer_id", data_type="integer", is_primary_key=True)],
            primary_key=["customer_id"],
        ),
    }
    return Schema(tables=tables)


def _partition_rows() -> list[dict]:
    return [
        {"table_name": "payment", "is_partitioned": True, "parent_name": None},
        {"table_name": "payment_p2022_06", "is_partitioned": False, "parent_name": "payment"},
        {"table_name": "payment_p2022_07", "is_partitioned": False, "parent_name": "payment"},
        {"table_name": "customer", "is_partitioned": False, "parent_name": None},
    ]


class TestAnnotatePartitionMetadata:
    def test_flags_parent_and_children(self):
        schema = _pagila_like_schema()
        annotate_partition_metadata(schema, _partition_rows())
        assert schema.tables["payment"].is_partitioned is True
        assert schema.tables["payment"].partition_of is None
        assert schema.tables["payment_p2022_06"].partition_of == "payment"
        assert schema.tables["payment_p2022_07"].partition_of == "payment"
        assert schema.tables["customer"].is_partitioned is False
        assert schema.tables["customer"].partition_of is None

    def test_rolls_child_fks_up_to_parent(self):
        schema = _pagila_like_schema()
        assert schema.tables["payment"].foreign_keys == []
        annotate_partition_metadata(schema, _partition_rows())
        parent_targets = sorted(fk.foreign_table for fk in schema.tables["payment"].foreign_keys)
        assert parent_targets == ["customer", "rental", "staff"]

    def test_rollup_is_deduplicated(self):
        # Two children with identical FK sets must not duplicate on the parent.
        schema = _pagila_like_schema()
        schema.tables["payment_p2022_07"] = _payment_partition("payment_p2022_07", with_fks=True)
        annotate_partition_metadata(schema, _partition_rows())
        assert len(schema.tables["payment"].foreign_keys) == 3


class TestGenerateConfigCollapsesPartitions:
    def test_children_collapsed_into_parent_by_default(self):
        schema = _pagila_like_schema()
        annotate_partition_metadata(schema, _partition_rows())
        config = ConfigManager.generate_default_config(schema)
        # Only the parent + the referenced table remain as collections.
        assert "payment" in config.collections
        assert "customer" in config.collections
        assert "payment_p2022_06" not in config.collections
        assert "payment_p2022_07" not in config.collections

    def test_parent_carries_the_edges(self):
        schema = _pagila_like_schema()
        annotate_partition_metadata(schema, _partition_rows())
        config = ConfigManager.generate_default_config(schema)
        from_collections = {e.from_collection for e in config.edges}
        assert from_collections == {"payment"}
        targets = sorted(e.to_collection for e in config.edges)
        assert targets == ["customer", "rental", "staff"]

    def test_expand_partitions_keeps_children(self):
        schema = _pagila_like_schema()
        annotate_partition_metadata(schema, _partition_rows())
        config = ConfigManager.generate_default_config(schema, expand_partitions=True)
        assert "payment_p2022_06" in config.collections
        assert "payment_p2022_07" in config.collections
