"""Tests for CDC output plugin parsers."""

from __future__ import annotations

import json

from r2g.cdc.models import ChangeOperation
from r2g.cdc.parsers import TestDecodingParser, Wal2JsonParser

# ======================================================================
# test_decoding parser
# ======================================================================


class TestTestDecodingInsert:
    def test_simple_insert(self):
        p = TestDecodingParser()
        msg = "table public.users: INSERT: id[int4]:1 name[text]:'Alice'"
        evt = p.parse_message(msg, lsn="0/100", xid=42)
        assert evt is not None
        assert evt.operation == ChangeOperation.INSERT
        assert evt.schema_name == "public"
        assert evt.table_name == "users"
        assert evt.new_row == {"id": 1, "name": "Alice"}
        assert evt.lsn == "0/100"
        assert evt.transaction_id == 42

    def test_insert_with_multiple_types(self):
        p = TestDecodingParser()
        msg = (
            "table public.products: INSERT: "
            "id[int4]:42 price[numeric]:19.99 "
            "active[bool]:true name[text]:'Widget'"
        )
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.new_row["id"] == 42
        assert evt.new_row["price"] == 19.99
        assert evt.new_row["active"] is True
        assert evt.new_row["name"] == "Widget"

    def test_insert_with_null(self):
        p = TestDecodingParser()
        msg = "table public.users: INSERT: id[int4]:1 email[text]:null"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.new_row["email"] is None

    def test_insert_quoted_with_spaces(self):
        p = TestDecodingParser()
        msg = "table public.users: INSERT: id[int4]:1 name[text]:'Alice Smith'"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.new_row["name"] == "Alice Smith"

    def test_insert_quoted_with_escaped_quote(self):
        p = TestDecodingParser()
        msg = "table public.users: INSERT: id[int4]:1 name[text]:'it''s fine'"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.new_row["name"] == "it's fine"

    def test_insert_with_boolean_false(self):
        p = TestDecodingParser()
        msg = "table public.flags: INSERT: id[int4]:1 flag[bool]:false"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.new_row["flag"] is False

    def test_insert_character_varying_type(self):
        p = TestDecodingParser()
        msg = "table public.users: INSERT: id[int4]:1 name[character varying]:'Bob'"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.new_row["name"] == "Bob"


class TestTestDecodingDelete:
    def test_simple_delete(self):
        p = TestDecodingParser()
        msg = "table public.users: DELETE: id[int4]:1"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.DELETE
        assert evt.old_row == {"id": 1}
        assert evt.new_row is None

    def test_delete_composite_key(self):
        p = TestDecodingParser()
        msg = "table public.enrollments: DELETE: student_id[int4]:10 course_id[int4]:20"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.old_row == {"student_id": 10, "course_id": 20}


class TestTestDecodingUpdate:
    def test_update_without_old_key(self):
        p = TestDecodingParser()
        msg = "table public.users: UPDATE: id[int4]:1 name[text]:'Bob'"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.UPDATE
        assert evt.old_row is None
        assert evt.new_row == {"id": 1, "name": "Bob"}

    def test_update_with_old_key_and_new_tuple(self):
        p = TestDecodingParser()
        msg = (
            "table public.users: UPDATE: "
            "old-key: id[int4]:1 "
            "new-tuple: id[int4]:1 name[text]:'Bob'"
        )
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.UPDATE
        assert evt.old_row == {"id": 1}
        assert evt.new_row == {"id": 1, "name": "Bob"}

    def test_update_old_key_only(self):
        p = TestDecodingParser()
        msg = "table public.users: UPDATE: old-key: id[int4]:1"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.old_row == {"id": 1}
        assert evt.new_row is None


class TestTestDecodingSkip:
    def test_begin_skipped(self):
        p = TestDecodingParser()
        assert p.parse_message("BEGIN 685") is None

    def test_commit_skipped(self):
        p = TestDecodingParser()
        assert p.parse_message("COMMIT 685") is None

    def test_garbage_returns_none(self):
        p = TestDecodingParser()
        assert p.parse_message("some random text") is None


class TestTestDecodingEdgeCases:
    def test_empty_string_value(self):
        p = TestDecodingParser()
        msg = "table public.users: INSERT: id[int4]:1 name[text]:''"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.new_row["name"] == ""

    def test_value_with_colon(self):
        p = TestDecodingParser()
        msg = "table public.urls: INSERT: id[int4]:1 url[text]:'http://example.com'"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.new_row["url"] == "http://example.com"

    def test_non_public_schema(self):
        p = TestDecodingParser()
        msg = "table myschema.users: INSERT: id[int4]:1"
        evt = p.parse_message(msg)
        assert evt is not None
        assert evt.schema_name == "myschema"


# ======================================================================
# wal2json parser
# ======================================================================


class TestWal2JsonInsert:
    def test_simple_insert(self):
        p = Wal2JsonParser()
        data = json.dumps({
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
        events = p.parse_message(data, lsn="0/100")
        assert len(events) == 1
        evt = events[0]
        assert evt.operation == ChangeOperation.INSERT
        assert evt.new_row == {"id": 1, "name": "Alice"}
        assert evt.transaction_id == 42
        assert evt.lsn == "0/100"


class TestWal2JsonUpdate:
    def test_update_with_old_keys(self):
        p = Wal2JsonParser()
        data = json.dumps({
            "change": [{
                "kind": "update",
                "schema": "public",
                "table": "users",
                "columnnames": ["id", "name"],
                "columntypes": ["integer", "text"],
                "columnvalues": [1, "Bob"],
                "oldkeys": {
                    "keynames": ["id"],
                    "keytypes": ["integer"],
                    "keyvalues": [1],
                },
            }],
        })
        events = p.parse_message(data)
        assert len(events) == 1
        evt = events[0]
        assert evt.operation == ChangeOperation.UPDATE
        assert evt.new_row == {"id": 1, "name": "Bob"}
        assert evt.old_row == {"id": 1}


class TestWal2JsonDelete:
    def test_delete_with_old_keys(self):
        p = Wal2JsonParser()
        data = json.dumps({
            "change": [{
                "kind": "delete",
                "schema": "public",
                "table": "users",
                "oldkeys": {
                    "keynames": ["id"],
                    "keytypes": ["integer"],
                    "keyvalues": [1],
                },
            }],
        })
        events = p.parse_message(data)
        assert len(events) == 1
        evt = events[0]
        assert evt.operation == ChangeOperation.DELETE
        assert evt.old_row == {"id": 1}
        assert evt.new_row is None


class TestWal2JsonMultipleChanges:
    def test_transaction_with_multiple_changes(self):
        p = Wal2JsonParser()
        data = json.dumps({
            "xid": 100,
            "change": [
                {
                    "kind": "insert",
                    "schema": "public",
                    "table": "users",
                    "columnnames": ["id", "name"],
                    "columntypes": ["integer", "text"],
                    "columnvalues": [1, "Alice"],
                },
                {
                    "kind": "insert",
                    "schema": "public",
                    "table": "orders",
                    "columnnames": ["id", "user_id", "total"],
                    "columntypes": ["integer", "integer", "numeric"],
                    "columnvalues": [10, 1, 99.99],
                },
            ],
        })
        events = p.parse_message(data)
        assert len(events) == 2
        assert events[0].table_name == "users"
        assert events[1].table_name == "orders"
        assert all(e.transaction_id == 100 for e in events)


class TestWal2JsonEdgeCases:
    def test_invalid_json(self):
        p = Wal2JsonParser()
        events = p.parse_message("not json")
        assert events == []

    def test_unknown_kind_skipped(self):
        p = Wal2JsonParser()
        data = json.dumps({
            "change": [{"kind": "truncate", "schema": "public", "table": "users"}],
        })
        events = p.parse_message(data)
        assert events == []

    def test_empty_change_list(self):
        p = Wal2JsonParser()
        data = json.dumps({"change": []})
        events = p.parse_message(data)
        assert events == []
