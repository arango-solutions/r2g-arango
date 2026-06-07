from __future__ import annotations

from r2g.keys import sanitize_key_component
from r2g.transformers.edge_transformer import EdgeTransformer
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import Column, EdgeDefinition, Table

# ArangoDB-legal key characters (everything else must be escaped).
_LEGAL = set(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "_-:.@()+,=;$!*'%"
)


class TestSanitizeKeyComponent:
    def test_clean_values_pass_through(self):
        assert sanitize_key_component(16051) == "16051"
        assert sanitize_key_component("abc-123") == "abc-123"

    def test_space_is_percent_encoded(self):
        # Pagila payment_date: spaces are illegal in ArangoDB keys.
        assert sanitize_key_component("2022-01-29 01:58:52.222594+00:00") == (
            "2022-01-29%2001:58:52.222594+00:00"
        )

    def test_result_contains_only_legal_characters(self):
        messy = "a b/c\\d?e#f\tg\u00e9h"
        out = sanitize_key_component(messy)
        assert all(ch in _LEGAL for ch in out), out

    def test_injective_no_collision_with_literal_percent(self):
        # "a b" must not collide with a literal "a%20b": % is always escaped.
        assert sanitize_key_component("a b") != sanitize_key_component("a%20b")


def _payment_table():
    # Mirrors a partitioned Pagila `payment`: composite PK (payment_date, payment_id).
    return Table(
        name="payment",
        columns=[
            Column(name="payment_id", data_type="integer", is_primary_key=True),
            Column(name="customer_id", data_type="integer"),
            Column(name="payment_date", data_type="timestamp", is_primary_key=True),
        ],
        primary_key=["payment_date", "payment_id"],
    )


class TestTimestampPkKeysAreLegalAndConsistent:
    def test_node_key_is_legal(self):
        nt = NodeTransformer(_payment_table())
        doc = nt.transform_row(
            {
                "payment_id": 16051,
                "customer_id": 269,
                "payment_date": "2022-01-29 01:58:52.222594+00:00",
            }
        )
        assert all(ch in _LEGAL for ch in doc["_key"]), doc["_key"]
        assert " " not in doc["_key"]

    def test_edge_from_matches_node_key(self):
        row = {
            "payment_id": 16051,
            "customer_id": 269,
            "payment_date": "2022-01-29 01:58:52.222594+00:00",
        }
        node_key = NodeTransformer(_payment_table()).transform_row(row)["_key"]

        edge_def = EdgeDefinition(
            edge_collection="payment_to_customer",
            from_collection="payment",
            to_collection="customer",
            from_field="customer_id",
            to_field="customer_id",
        )
        edge = EdgeTransformer(edge_def, _payment_table()).transform_row(row)
        # The edge's _from key portion must equal the vertex _key so the
        # endpoint resolves during graph traversal.
        assert edge["_from"] == f"payment/{node_key}"
        assert all(ch in _LEGAL for ch in edge["_from"].split("/", 1)[1])
