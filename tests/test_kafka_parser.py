"""Tests for Debezium and flat JSON Kafka message parsers."""

from __future__ import annotations

import json

import pytest

from r2g.cdc.kafka_parser import DebeziumParser, FlatJsonParser
from r2g.cdc.models import ChangeOperation


class TestDebeziumParser:
    @pytest.fixture
    def parser(self):
        return DebeziumParser()

    def test_insert(self, parser):
        msg = json.dumps({
            "before": None,
            "after": {"id": 1, "name": "Alice"},
            "source": {"schema": "public", "table": "users", "lsn": 12345, "txId": 42},
            "op": "c",
            "ts_ms": 1700000000000,
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.INSERT
        assert evt.table_name == "users"
        assert evt.schema_name == "public"
        assert evt.new_row == {"id": 1, "name": "Alice"}
        assert evt.old_row is None
        assert evt.lsn == "12345"
        assert evt.transaction_id == 42

    def test_update(self, parser):
        msg = json.dumps({
            "before": {"id": 1, "name": "Alice"},
            "after": {"id": 1, "name": "Bob"},
            "source": {"schema": "public", "table": "users", "lsn": 12346},
            "op": "u",
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.UPDATE
        assert evt.old_row == {"id": 1, "name": "Alice"}
        assert evt.new_row == {"id": 1, "name": "Bob"}

    def test_delete(self, parser):
        msg = json.dumps({
            "before": {"id": 1, "name": "Alice"},
            "after": None,
            "source": {"schema": "public", "table": "users"},
            "op": "d",
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.DELETE
        assert evt.old_row == {"id": 1, "name": "Alice"}
        assert evt.new_row is None

    def test_snapshot_read(self, parser):
        msg = json.dumps({
            "before": None,
            "after": {"id": 1, "name": "Alice"},
            "source": {"schema": "public", "table": "users"},
            "op": "r",
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.INSERT

    def test_wrapped_payload(self, parser):
        msg = json.dumps({
            "schema": {},
            "payload": {
                "before": None,
                "after": {"id": 1},
                "source": {"schema": "public", "table": "users"},
                "op": "c",
            },
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.INSERT
        assert evt.new_row == {"id": 1}

    def test_bytes_input(self, parser):
        raw = json.dumps({
            "before": None,
            "after": {"id": 1},
            "source": {"schema": "public", "table": "users"},
            "op": "c",
        }).encode("utf-8")
        evt = parser.parse(raw)
        assert evt is not None
        assert evt.operation == ChangeOperation.INSERT

    def test_dict_input(self, parser):
        payload = {
            "before": None,
            "after": {"id": 1},
            "source": {"schema": "public", "table": "users"},
            "op": "c",
        }
        evt = parser.parse(payload)
        assert evt is not None

    def test_invalid_json(self, parser):
        evt = parser.parse("not json at all")
        assert evt is None

    def test_no_op_field(self, parser):
        msg = json.dumps({"before": None, "after": {"id": 1}})
        evt = parser.parse(msg)
        assert evt is None

    def test_unknown_op(self, parser):
        msg = json.dumps({
            "before": None,
            "after": {"id": 1},
            "source": {"table": "users"},
            "op": "x",
        })
        evt = parser.parse(msg)
        assert evt is None

    def test_no_table(self, parser):
        msg = json.dumps({
            "before": None,
            "after": {"id": 1},
            "source": {"schema": "public"},
            "op": "c",
        })
        evt = parser.parse(msg)
        assert evt is None

    def test_default_schema(self):
        parser = DebeziumParser(default_schema="myschema")
        msg = json.dumps({
            "before": None,
            "after": {"id": 1},
            "source": {"table": "users"},
            "op": "c",
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.schema_name == "myschema"

    def test_no_lsn(self, parser):
        msg = json.dumps({
            "before": None,
            "after": {"id": 1},
            "source": {"table": "users"},
            "op": "c",
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.lsn is None

    def test_parse_batch(self, parser):
        msgs = [
            json.dumps({
                "before": None, "after": {"id": i},
                "source": {"table": "users"}, "op": "c",
            })
            for i in range(3)
        ]
        msgs.append("invalid json")
        events = parser.parse_batch(msgs)
        assert len(events) == 3


class TestFlatJsonParser:
    @pytest.fixture
    def parser(self):
        return FlatJsonParser()

    def test_insert(self, parser):
        msg = json.dumps({
            "operation": "INSERT",
            "table_name": "users",
            "new_row": {"id": 1, "name": "Alice"},
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.INSERT
        assert evt.table_name == "users"

    def test_update(self, parser):
        msg = json.dumps({
            "operation": "UPDATE",
            "table_name": "users",
            "new_row": {"id": 1, "name": "Bob"},
            "old_row": {"id": 1, "name": "Alice"},
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.UPDATE

    def test_delete(self, parser):
        msg = json.dumps({
            "operation": "DELETE",
            "table_name": "users",
            "old_row": {"id": 1},
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.DELETE

    def test_case_insensitive_op(self, parser):
        msg = json.dumps({
            "operation": "insert",
            "table_name": "users",
            "new_row": {"id": 1},
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.INSERT

    def test_invalid_json(self, parser):
        evt = parser.parse("garbage")
        assert evt is None

    def test_unknown_op(self, parser):
        msg = json.dumps({"operation": "TRUNCATE", "table_name": "users"})
        evt = parser.parse(msg)
        assert evt is None

    def test_no_table(self, parser):
        msg = json.dumps({"operation": "INSERT", "new_row": {"id": 1}})
        evt = parser.parse(msg)
        assert evt is None

    def test_with_lsn_and_txid(self, parser):
        msg = json.dumps({
            "operation": "INSERT",
            "table_name": "users",
            "new_row": {"id": 1},
            "lsn": "0/ABC",
            "transaction_id": 42,
            "schema_name": "myschema",
        })
        evt = parser.parse(msg)
        assert evt is not None
        assert evt.lsn == "0/ABC"
        assert evt.transaction_id == 42
        assert evt.schema_name == "myschema"

    def test_bytes_input(self, parser):
        raw = json.dumps({
            "operation": "INSERT",
            "table_name": "users",
            "new_row": {"id": 1},
        }).encode()
        evt = parser.parse(raw)
        assert evt is not None
