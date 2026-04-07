"""Tests for CDC conflict resolution."""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock

import pytest

from r2g.cdc.conflict import (
    ConflictEvent,
    ConflictLog,
    ConflictPolicy,
    ConflictResolver,
    ConflictType,
)
from r2g.cdc.handler import CDCHandler
from r2g.cdc.models import (
    ChangeEvent,
    ChangeOperation,
)
from r2g.connectors.arango_writer import ArangoWriter
from r2g.types import (
    CollectionMapping,
    Column,
    MappingConfig,
    Schema,
    Table,
)


@pytest.fixture
def mock_writer():
    w = MagicMock(spec=ArangoWriter)
    w.insert_document = MagicMock(return_value={"_key": "1"})
    w.replace_document = MagicMock(return_value={"_key": "1"})
    w.delete_document = MagicMock(return_value=True)
    w.ensure_collection = MagicMock()
    w.apply_delta = MagicMock()
    mock_db = MagicMock()
    type(w).db = PropertyMock(return_value=mock_db)
    return w


# ======================================================================
# ConflictLog
# ======================================================================


class TestConflictLog:
    def test_empty_log(self):
        log = ConflictLog()
        assert log.total == 0
        assert log.counts() == {}

    def test_record_and_count(self):
        log = ConflictLog()
        log.record(ConflictEvent(
            conflict_type=ConflictType.INSERT_DUPLICATE,
            collection="users",
            key="1",
            policy=ConflictPolicy.SOURCE_WINS,
        ))
        log.record(ConflictEvent(
            conflict_type=ConflictType.INSERT_DUPLICATE,
            collection="users",
            key="2",
            policy=ConflictPolicy.SOURCE_WINS,
        ))
        log.record(ConflictEvent(
            conflict_type=ConflictType.REPLACE_MISSING,
            collection="orders",
            key="5",
            policy=ConflictPolicy.SOURCE_WINS,
        ))
        assert log.total == 3
        assert log.counts() == {
            "insert_duplicate": 2,
            "replace_missing": 1,
        }

    def test_summary(self):
        log = ConflictLog()
        log.record(ConflictEvent(
            conflict_type=ConflictType.DELETE_MISSING,
            collection="users",
            policy=ConflictPolicy.LOG_AND_SKIP,
        ))
        s = log.summary()
        assert s["total_conflicts"] == 1
        assert "delete_missing" in s["by_type"]


# ======================================================================
# ConflictResolver -- SOURCE_WINS
# ======================================================================


class TestSourceWinsInsert:
    def test_clean_insert(self, mock_writer):
        resolver = ConflictResolver(ConflictPolicy.SOURCE_WINS)
        ok = resolver.resolve_insert(mock_writer, "users", {"_key": "1", "name": "Alice"})
        assert ok is True
        mock_writer.insert_document.assert_called_once()
        assert resolver.log.total == 0

    def test_duplicate_insert_upserts(self, mock_writer):
        mock_writer.insert_document.side_effect = Exception("unique constraint violated (1210)")
        resolver = ConflictResolver(ConflictPolicy.SOURCE_WINS)
        ok = resolver.resolve_insert(mock_writer, "users", {"_key": "1", "name": "Alice"})
        assert ok is True
        mock_writer.replace_document.assert_called_once()
        assert resolver.log.total == 1
        assert resolver.log.events[0].conflict_type == ConflictType.INSERT_DUPLICATE
        assert resolver.log.events[0].resolved is True


class TestSourceWinsReplace:
    def test_clean_replace(self, mock_writer):
        resolver = ConflictResolver(ConflictPolicy.SOURCE_WINS)
        ok = resolver.resolve_replace(mock_writer, "users", {"_key": "1", "name": "Bob"})
        assert ok is True
        mock_writer.replace_document.assert_called_once()

    def test_missing_replace_inserts(self, mock_writer):
        mock_writer.replace_document.side_effect = Exception("document not found (1202)")
        resolver = ConflictResolver(ConflictPolicy.SOURCE_WINS)
        ok = resolver.resolve_replace(mock_writer, "users", {"_key": "1", "name": "Bob"})
        assert ok is True
        mock_writer.insert_document.assert_called_once()
        assert resolver.log.total == 1
        assert resolver.log.events[0].conflict_type == ConflictType.REPLACE_MISSING


class TestSourceWinsDelete:
    def test_clean_delete(self, mock_writer):
        resolver = ConflictResolver(ConflictPolicy.SOURCE_WINS)
        ok = resolver.resolve_delete(mock_writer, "users", "1")
        assert ok is True
        mock_writer.delete_document.assert_called_once_with("users", "1")


# ======================================================================
# ConflictResolver -- LOG_AND_SKIP
# ======================================================================


class TestLogAndSkipInsert:
    def test_duplicate_logged_and_skipped(self, mock_writer):
        mock_writer.insert_document.side_effect = Exception("unique constraint violated")
        resolver = ConflictResolver(ConflictPolicy.LOG_AND_SKIP)
        ok = resolver.resolve_insert(mock_writer, "users", {"_key": "1"})
        assert ok is False
        mock_writer.replace_document.assert_not_called()
        assert resolver.log.total == 1
        assert resolver.log.events[0].resolution == "skipped"


class TestLogAndSkipReplace:
    def test_missing_logged_and_skipped(self, mock_writer):
        mock_writer.replace_document.side_effect = Exception("document not found")
        resolver = ConflictResolver(ConflictPolicy.LOG_AND_SKIP)
        ok = resolver.resolve_replace(mock_writer, "users", {"_key": "1"})
        assert ok is False
        mock_writer.insert_document.assert_not_called()
        assert resolver.log.total == 1


# ======================================================================
# ConflictResolver -- FAIL
# ======================================================================


class TestFailPolicy:
    def test_duplicate_raises(self, mock_writer):
        mock_writer.insert_document.side_effect = Exception("unique constraint violated")
        resolver = ConflictResolver(ConflictPolicy.FAIL)
        with pytest.raises(Exception, match="unique constraint"):
            resolver.resolve_insert(mock_writer, "users", {"_key": "1"})
        assert resolver.log.total == 1

    def test_missing_replace_raises(self, mock_writer):
        mock_writer.replace_document.side_effect = Exception("document not found")
        resolver = ConflictResolver(ConflictPolicy.FAIL)
        with pytest.raises(Exception, match="not found"):
            resolver.resolve_replace(mock_writer, "users", {"_key": "1"})


# ======================================================================
# ConflictResolver -- LAST_WRITE_WINS
# ======================================================================


class TestLastWriteWins:
    def test_duplicate_newer_overwrites(self, mock_writer):
        mock_writer.insert_document.side_effect = Exception("unique constraint violated (1210)")
        mock_coll = MagicMock()
        mock_coll.get.return_value = {"_key": "1", "_r2g_lsn": "0/100"}
        mock_writer.db.collection.return_value = mock_coll

        resolver = ConflictResolver(ConflictPolicy.LAST_WRITE_WINS)
        ok = resolver.resolve_insert(
            mock_writer, "users", {"_key": "1", "name": "Alice"}, lsn="0/200"
        )
        assert ok is True
        mock_writer.replace_document.assert_called_once()
        assert resolver.log.events[0].resolution == "overwritten (newer LSN)"

    def test_duplicate_stale_skipped(self, mock_writer):
        mock_writer.insert_document.side_effect = Exception("unique constraint violated (1210)")
        mock_coll = MagicMock()
        mock_coll.get.return_value = {"_key": "1", "_r2g_lsn": "0/300"}
        mock_writer.db.collection.return_value = mock_coll

        resolver = ConflictResolver(ConflictPolicy.LAST_WRITE_WINS)
        ok = resolver.resolve_insert(
            mock_writer, "users", {"_key": "1", "name": "Alice"}, lsn="0/100"
        )
        assert ok is False
        assert resolver.log.events[0].resolution == "skipped (stale LSN)"

    def test_replace_stamps_lsn(self, mock_writer):
        resolver = ConflictResolver(ConflictPolicy.LAST_WRITE_WINS)
        ok = resolver.resolve_replace(
            mock_writer, "users", {"_key": "1", "name": "Bob"}, lsn="0/200"
        )
        assert ok is True
        call_args = mock_writer.replace_document.call_args[0]
        assert call_args[1]["_r2g_lsn"] == "0/200"


# ======================================================================
# Non-conflict errors propagate
# ======================================================================


class TestNonConflictErrors:
    def test_connection_error_propagates(self, mock_writer):
        mock_writer.insert_document.side_effect = ConnectionError("lost connection")
        resolver = ConflictResolver(ConflictPolicy.SOURCE_WINS)
        with pytest.raises(ConnectionError):
            resolver.resolve_insert(mock_writer, "users", {"_key": "1"})

    def test_server_error_propagates(self, mock_writer):
        mock_writer.replace_document.side_effect = RuntimeError("server 500")
        resolver = ConflictResolver(ConflictPolicy.SOURCE_WINS)
        with pytest.raises(RuntimeError):
            resolver.resolve_replace(mock_writer, "users", {"_key": "1"})


# ======================================================================
# CDCHandler with conflict policy integration
# ======================================================================


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


class TestHandlerConflictIntegration:
    def test_default_policy_is_source_wins(self, mock_writer, schema, config):
        handler = CDCHandler(mock_writer, schema, config)
        assert handler.resolver.policy == ConflictPolicy.SOURCE_WINS

    def test_custom_policy(self, mock_writer, schema, config):
        handler = CDCHandler(
            mock_writer, schema, config,
            conflict_policy=ConflictPolicy.LOG_AND_SKIP,
        )
        assert handler.resolver.policy == ConflictPolicy.LOG_AND_SKIP

    def test_handler_tracks_skipped(self, mock_writer, schema, config):
        mock_writer.insert_document.side_effect = Exception("unique constraint violated")
        handler = CDCHandler(
            mock_writer, schema, config,
            conflict_policy=ConflictPolicy.LOG_AND_SKIP,
        )
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="users",
            new_row={"id": 1, "name": "Alice"},
        )
        handler.handle_event(evt)
        assert handler.stats.deltas_skipped == 1
        assert handler.stats.deltas_applied == 0

    def test_stats_includes_skipped(self, mock_writer, schema, config):
        handler = CDCHandler(mock_writer, schema, config)
        d = handler.stats.as_dict()
        assert "deltas_skipped" in d
