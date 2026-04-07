"""Tests for the CDC handler."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from r2g.cdc.handler import CDCHandler, CDCStats
from r2g.cdc.models import (
    ArangoOperation,
    ChangeEvent,
    ChangeOperation,
)
from r2g.connectors.arango_writer import ArangoWriter
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
def schema() -> Schema:
    return Schema(tables={
        "users": Table(
            name="users",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="name", data_type="text"),
            ],
            primary_key=["id"],
        ),
        "orders": Table(
            name="orders",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="user_id", data_type="integer"),
                Column(name="total", data_type="numeric"),
            ],
            primary_key=["id"],
            foreign_keys=[
                ForeignKey(column="user_id", foreign_table="users", foreign_column="id"),
            ],
        ),
    })


@pytest.fixture
def config() -> MappingConfig:
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
def mock_writer():
    writer = MagicMock(spec=ArangoWriter)
    writer.ensure_collection = MagicMock()
    writer.insert_document = MagicMock(return_value={"_key": "1"})
    writer.replace_document = MagicMock(return_value={"_key": "1"})
    writer.delete_document = MagicMock(return_value=True)
    writer.apply_delta = MagicMock()
    return writer


class TestHandleEvent:
    def test_insert_event(self, schema, config, mock_writer):
        handler = CDCHandler(mock_writer, schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="users",
            new_row={"id": 1, "name": "Alice"},
            lsn="0/ABC",
        )
        deltas = handler.handle_event(evt)
        assert len(deltas) == 1
        assert handler.stats.events_received == 1
        assert handler.stats.deltas_applied == 1
        assert handler.stats.last_lsn == "0/ABC"
        mock_writer.apply_delta.assert_called_once()

    def test_insert_with_edge(self, schema, config, mock_writer):
        handler = CDCHandler(mock_writer, schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="orders",
            new_row={"id": 5, "user_id": 1, "total": 99.99},
        )
        deltas = handler.handle_event(evt)
        assert len(deltas) == 2
        assert handler.stats.deltas_applied == 2
        assert mock_writer.apply_delta.call_count == 2

    def test_delete_event(self, schema, config, mock_writer):
        handler = CDCHandler(mock_writer, schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.DELETE,
            table_name="users",
            old_row={"id": 1, "name": "Alice"},
        )
        deltas = handler.handle_event(evt)
        assert len(deltas) == 1
        assert deltas[0].operation == ArangoOperation.DELETE


class TestHandleEvents:
    def test_multiple_events(self, schema, config, mock_writer):
        handler = CDCHandler(mock_writer, schema, config)
        events = [
            ChangeEvent(
                operation=ChangeOperation.INSERT,
                table_name="users",
                new_row={"id": 1, "name": "Alice"},
            ),
            ChangeEvent(
                operation=ChangeOperation.INSERT,
                table_name="users",
                new_row={"id": 2, "name": "Bob"},
            ),
        ]
        stats = handler.handle_events(events)
        assert stats.events_received == 2
        assert stats.deltas_applied == 2


class TestHandleTransaction:
    def test_transaction_batch(self, schema, config, mock_writer):
        handler = CDCHandler(mock_writer, schema, config)
        events = [
            ChangeEvent(
                operation=ChangeOperation.INSERT,
                table_name="users",
                new_row={"id": 1, "name": "Alice"},
                transaction_id=42,
                lsn="0/100",
            ),
            ChangeEvent(
                operation=ChangeOperation.INSERT,
                table_name="orders",
                new_row={"id": 10, "user_id": 1, "total": 50.0},
                transaction_id=42,
                lsn="0/101",
            ),
        ]
        batch = handler.handle_transaction(events)
        assert batch.transaction_id == 42
        assert batch.lsn == "0/101"
        assert batch.source_events == 2
        assert len(batch.deltas) == 3
        assert handler.stats.transactions_completed == 1


class TestGroupByTransaction:
    def test_groups_by_txn_id(self, schema, config, mock_writer):
        handler = CDCHandler(mock_writer, schema, config)
        events = [
            ChangeEvent(operation=ChangeOperation.INSERT, table_name="users", new_row={"id": 1}, transaction_id=1),
            ChangeEvent(operation=ChangeOperation.INSERT, table_name="users", new_row={"id": 2}, transaction_id=2),
            ChangeEvent(operation=ChangeOperation.INSERT, table_name="users", new_row={"id": 3}, transaction_id=1),
        ]
        groups = handler.group_by_transaction(events)
        assert len(groups) == 2
        assert len(groups[0]) == 2
        assert groups[0][0].new_row["id"] == 1
        assert groups[0][1].new_row["id"] == 3
        assert len(groups[1]) == 1

    def test_no_txn_id_individual_groups(self, schema, config, mock_writer):
        handler = CDCHandler(mock_writer, schema, config)
        events = [
            ChangeEvent(operation=ChangeOperation.INSERT, table_name="users", new_row={"id": 1}),
            ChangeEvent(operation=ChangeOperation.INSERT, table_name="users", new_row={"id": 2}),
        ]
        groups = handler.group_by_transaction(events)
        assert len(groups) == 2
        assert all(len(g) == 1 for g in groups)


class TestDeltaFailure:
    def test_failed_delta_increments_counter(self, schema, config):
        writer = MagicMock(spec=ArangoWriter)
        writer.apply_delta = MagicMock(side_effect=RuntimeError("connection lost"))
        handler = CDCHandler(writer, schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="users",
            new_row={"id": 1, "name": "Alice"},
        )
        deltas = handler.handle_event(evt)
        assert len(deltas) == 1
        assert handler.stats.deltas_failed == 1
        assert handler.stats.deltas_applied == 0


class TestProgressCallback:
    def test_callback_invoked(self, schema, config, mock_writer):
        calls = []
        handler = CDCHandler(mock_writer, schema, config, on_progress=lambda *a: calls.append(a))
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="users",
            new_row={"id": 1, "name": "Alice"},
        )
        handler.handle_event(evt)
        assert len(calls) == 1
        assert calls[0] == ("event", 1, 1)


class TestCDCStats:
    def test_as_dict(self):
        stats = CDCStats()
        stats.events_received = 5
        stats.deltas_applied = 10
        stats.last_lsn = "0/ABC"
        d = stats.as_dict()
        assert d["events_received"] == 5
        assert d["deltas_applied"] == 10
        assert d["last_lsn"] == "0/ABC"
