"""Tests for PGReplicationListener (mocked PostgreSQL)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from r2g.cdc.handler import CDCHandler
from r2g.cdc.models import ChangeOperation
from r2g.cdc.pg_listener import SUPPORTED_PLUGINS, PGReplicationListener
from r2g.connectors.arango_writer import ArangoWriter
from r2g.types import (
    CollectionMapping,
    Column,
    MappingConfig,
    Schema,
    Table,
)


@pytest.fixture
def schema():
    return Schema(tables={
        "users": Table(
            name="users",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="name", data_type="text"),
            ],
            primary_key=["id"],
        ),
    })


@pytest.fixture
def config():
    return MappingConfig(
        collections={
            "users": CollectionMapping(source_table="users", target_collection="users"),
        },
    )


@pytest.fixture
def mock_writer():
    w = MagicMock(spec=ArangoWriter)
    w.apply_delta = MagicMock()
    w.ensure_collection = MagicMock()
    return w


@pytest.fixture
def handler(mock_writer, schema, config):
    return CDCHandler(mock_writer, schema, config)


class TestInit:
    def test_valid_plugins(self):
        assert "test_decoding" in SUPPORTED_PLUGINS
        assert "wal2json" in SUPPORTED_PLUGINS

    def test_invalid_plugin_raises(self, handler):
        with pytest.raises(ValueError, match="Unsupported plugin"):
            PGReplicationListener("postgresql://test", handler, plugin="nope")


class TestSlotManagement:
    @patch("r2g.cdc.pg_listener.psycopg")
    def test_create_slot_new(self, mock_psycopg, handler):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [None, ("0/0",)]
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg.connect.return_value = mock_conn

        listener = PGReplicationListener("postgresql://test", handler)
        created = listener.create_slot()
        assert created is True

    @patch("r2g.cdc.pg_listener.psycopg")
    def test_create_slot_exists(self, mock_psycopg, handler):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg.connect.return_value = mock_conn

        listener = PGReplicationListener("postgresql://test", handler)
        created = listener.create_slot()
        assert created is False

    @patch("r2g.cdc.pg_listener.psycopg")
    def test_drop_slot(self, mock_psycopg, handler):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg.connect.return_value = mock_conn

        listener = PGReplicationListener("postgresql://test", handler)
        dropped = listener.drop_slot()
        assert dropped is True

    @patch("r2g.cdc.pg_listener.psycopg")
    def test_drop_slot_not_found(self, mock_psycopg, handler):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg.connect.return_value = mock_conn

        listener = PGReplicationListener("postgresql://test", handler)
        dropped = listener.drop_slot()
        assert dropped is False

    @patch("r2g.cdc.pg_listener.psycopg")
    def test_slot_status(self, mock_psycopg, handler):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            "r2g_slot", "test_decoding", "logical", False, "0/100", "0/100"
        )
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg.connect.return_value = mock_conn

        listener = PGReplicationListener("postgresql://test", handler)
        status = listener.slot_status()
        assert status is not None
        assert status["slot_name"] == "r2g_slot"
        assert status["plugin"] == "test_decoding"
        assert status["slot_type"] == "logical"

    @patch("r2g.cdc.pg_listener.psycopg")
    def test_slot_status_not_found(self, mock_psycopg, handler):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg.connect.return_value = mock_conn

        listener = PGReplicationListener("postgresql://test", handler)
        assert listener.slot_status() is None


class TestPollOnce:
    @patch("r2g.cdc.pg_listener.psycopg")
    def test_poll_test_decoding(self, mock_psycopg, handler):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("0/100", 42, "BEGIN 42"),
            ("0/101", 42, "table public.users: INSERT: id[int4]:1 name[text]:'Alice'"),
            ("0/102", 42, "COMMIT 42"),
        ]
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.closed = False
        mock_psycopg.connect.return_value = mock_conn

        listener = PGReplicationListener("postgresql://test", handler)
        listener._conn = mock_conn
        events = listener.poll_once()

        assert len(events) == 1
        assert events[0].operation == ChangeOperation.INSERT
        assert events[0].table_name == "users"
        assert events[0].new_row["name"] == "Alice"
        assert events[0].lsn == "0/101"

    @patch("r2g.cdc.pg_listener.psycopg")
    def test_poll_wal2json(self, mock_psycopg, handler):
        import json

        mock_conn = MagicMock()
        wal2json_data = json.dumps({
            "xid": 42,
            "change": [{
                "kind": "insert",
                "schema": "public",
                "table": "users",
                "columnnames": ["id", "name"],
                "columntypes": ["integer", "text"],
                "columnvalues": [1, "Alice"],
            }],
        })
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("0/100", 42, wal2json_data),
        ]
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.closed = False
        mock_psycopg.connect.return_value = mock_conn

        listener = PGReplicationListener(
            "postgresql://test", handler, plugin="wal2json"
        )
        listener._conn = mock_conn
        events = listener.poll_once()

        assert len(events) == 1
        assert events[0].table_name == "users"
        assert events[0].new_row["name"] == "Alice"

    @patch("r2g.cdc.pg_listener.psycopg")
    def test_poll_empty(self, mock_psycopg, handler):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.closed = False
        mock_psycopg.connect.return_value = mock_conn

        listener = PGReplicationListener("postgresql://test", handler)
        listener._conn = mock_conn
        events = listener.poll_once()
        assert events == []


class TestRunLoop:
    @patch("r2g.cdc.pg_listener.time")
    @patch("r2g.cdc.pg_listener.psycopg")
    def test_run_processes_events_then_stops(self, mock_psycopg, mock_time, handler):
        mock_conn = MagicMock()
        mock_conn.closed = False
        call_count = 0

        def mock_fetchall():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [
                    ("0/100", 42, "table public.users: INSERT: id[int4]:1 name[text]:'Alice'"),
                ]
            return []

        mock_cursor = MagicMock()
        mock_cursor.fetchall = mock_fetchall
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg.connect.return_value = mock_conn

        def stop_after_sleep(*args):
            listener.stop()

        mock_time.sleep.side_effect = stop_after_sleep

        listener = PGReplicationListener("postgresql://test", handler)
        listener._conn = mock_conn
        listener.run()

        assert handler.stats.events_received == 1

    @patch("r2g.cdc.pg_listener.time")
    @patch("r2g.cdc.pg_listener.psycopg")
    def test_stop_method(self, mock_psycopg, mock_time, handler):
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg.connect.return_value = mock_conn

        mock_time.sleep.side_effect = lambda _: listener.stop()

        listener = PGReplicationListener("postgresql://test", handler)
        listener._conn = mock_conn
        listener.run()
        assert not listener._running


class TestSetupTeardown:
    @patch("r2g.cdc.pg_listener.psycopg")
    def test_setup_creates_and_returns_status(self, mock_psycopg, handler):
        mock_conn = MagicMock()

        # setup() calls: slot_exists → create_slot → slot_status
        # fetchone sequence: (1) None for slot_exists, (2) status row for slot_status
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            None,  # slot_exists check → not found
            ("r2g_slot", "test_decoding", "logical", False, "0/100", "0/100"),
        ]
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg.connect.return_value = mock_conn

        listener = PGReplicationListener("postgresql://test", handler)
        status = listener.setup()
        assert status["slot_name"] == "r2g_slot"

    @patch("r2g.cdc.pg_listener.psycopg")
    def test_teardown_drops_and_closes(self, mock_psycopg, handler):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.closed = False
        mock_psycopg.connect.return_value = mock_conn

        listener = PGReplicationListener("postgresql://test", handler)
        listener._conn = mock_conn
        listener.teardown(drop_slot=True)
        mock_conn.close.assert_called_once()
