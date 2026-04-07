"""Tests for CDC event models."""

from __future__ import annotations

from datetime import datetime

from r2g.cdc.models import (
    ArangoDelta,
    ArangoOperation,
    ChangeEvent,
    ChangeOperation,
    TransactionBatch,
)


class TestChangeEvent:
    def test_insert_event(self):
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="users",
            new_row={"id": 1, "name": "Alice"},
        )
        assert evt.is_insert
        assert not evt.is_update
        assert not evt.is_delete
        assert evt.effective_row == {"id": 1, "name": "Alice"}

    def test_update_event(self):
        evt = ChangeEvent(
            operation=ChangeOperation.UPDATE,
            table_name="users",
            old_row={"id": 1, "name": "Alice"},
            new_row={"id": 1, "name": "Bob"},
        )
        assert evt.is_update
        assert evt.effective_row == {"id": 1, "name": "Bob"}

    def test_delete_event(self):
        evt = ChangeEvent(
            operation=ChangeOperation.DELETE,
            table_name="users",
            old_row={"id": 1, "name": "Alice"},
        )
        assert evt.is_delete
        assert evt.effective_row == {"id": 1, "name": "Alice"}

    def test_delete_without_old_row(self):
        evt = ChangeEvent(
            operation=ChangeOperation.DELETE,
            table_name="users",
        )
        assert evt.effective_row == {}

    def test_lsn_and_timestamp(self):
        ts = datetime(2026, 4, 1, 12, 0, 0)
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="users",
            new_row={"id": 1},
            lsn="0/1234ABC",
            timestamp=ts,
            transaction_id=42,
        )
        assert evt.lsn == "0/1234ABC"
        assert evt.timestamp == ts
        assert evt.transaction_id == 42

    def test_default_schema(self):
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="users",
            new_row={"id": 1},
        )
        assert evt.schema_name == "public"


class TestArangoDelta:
    def test_insert_delta(self):
        delta = ArangoDelta(
            operation=ArangoOperation.INSERT,
            collection="users",
            document={"_key": "1", "name": "Alice"},
        )
        assert delta.effective_key == "1"
        assert not delta.is_edge

    def test_explicit_key_overrides_document(self):
        delta = ArangoDelta(
            operation=ArangoOperation.DELETE,
            collection="users",
            key="42",
            document={"_key": "99"},
        )
        assert delta.effective_key == "42"

    def test_edge_delta(self):
        delta = ArangoDelta(
            operation=ArangoOperation.INSERT,
            collection="orders_to_users",
            is_edge=True,
            document={"_key": "1_10", "_from": "orders/1", "_to": "users/10"},
        )
        assert delta.is_edge
        assert delta.effective_key == "1_10"

    def test_empty_key(self):
        delta = ArangoDelta(
            operation=ArangoOperation.INSERT,
            collection="logs",
            document={"message": "hello"},
        )
        assert delta.effective_key == ""


class TestTransactionBatch:
    def test_empty_batch(self):
        batch = TransactionBatch()
        assert batch.deltas == []
        assert batch.source_events == 0
        assert batch.transaction_id is None

    def test_batch_with_deltas(self):
        d1 = ArangoDelta(operation=ArangoOperation.INSERT, collection="users")
        d2 = ArangoDelta(operation=ArangoOperation.INSERT, collection="orders")
        batch = TransactionBatch(
            transaction_id=42,
            lsn="0/ABC",
            deltas=[d1, d2],
            source_events=2,
        )
        assert len(batch.deltas) == 2
        assert batch.transaction_id == 42
