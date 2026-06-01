from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from r2g.connectors.kafka_source import KafkaConnector

AVRO_SCHEMA = json.dumps({
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "customer", "type": "string"},
        {"name": "total", "type": "double"},
        {"name": "note", "type": ["null", "string"]},
        {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "active", "type": "boolean"},
    ],
})

JSON_SCHEMA = json.dumps({
    "type": "object",
    "required": ["id", "name"],
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "score": {"type": "number"},
        "tags": {"type": "array"},
        "nickname": {"type": ["null", "string"]},
    },
})


def _connector_with_schema(monkeypatch, schema_str):
    conn = KafkaConnector(
        "localhost:9092",
        schema_registry_url="http://localhost:8081",
        topic="orders",
    )
    fake_client = MagicMock()
    registered = MagicMock()
    registered.schema.schema_str = schema_str
    fake_client.get_latest_version.return_value = registered
    monkeypatch.setattr(conn, "_registry_client", lambda: fake_client)
    return conn, fake_client


class TestKafkaInit:
    def test_default_subject(self):
        conn = KafkaConnector("b:9092", schema_registry_url="http://r", topic="orders")
        assert conn.subject == "orders-value"

    def test_explicit_subject(self):
        conn = KafkaConnector("b:9092", schema_registry_url="http://r", topic="orders", subject="custom")
        assert conn.subject == "custom"

    def test_requires_registry_url(self):
        with pytest.raises(ValueError, match="schema_registry_url"):
            KafkaConnector("b:9092", schema_registry_url="", topic="orders")

    def test_requires_topic(self):
        with pytest.raises(ValueError, match="topic"):
            KafkaConnector("b:9092", schema_registry_url="http://r", topic="")


class TestKafkaAvroIntrospection:
    def test_parses_avro_record(self, monkeypatch):
        conn, client = _connector_with_schema(monkeypatch, AVRO_SCHEMA)
        schema = conn.get_schema()
        client.get_latest_version.assert_called_once_with("orders-value")
        assert set(schema.tables) == {"orders"}
        cols = {c.name: c for c in schema.tables["orders"].columns}
        assert cols["id"].data_type == "integer"
        assert cols["customer"].data_type == "text"
        assert cols["total"].data_type == "double precision"
        assert cols["created_at"].data_type == "timestamp"
        assert cols["active"].data_type == "boolean"

    def test_union_with_null_is_nullable(self, monkeypatch):
        conn, _ = _connector_with_schema(monkeypatch, AVRO_SCHEMA)
        schema = conn.get_schema()
        cols = {c.name: c for c in schema.tables["orders"].columns}
        assert cols["note"].is_nullable is True
        assert cols["id"].is_nullable is False


class TestKafkaJsonSchemaIntrospection:
    def test_parses_json_schema(self, monkeypatch):
        conn, _ = _connector_with_schema(monkeypatch, JSON_SCHEMA)
        schema = conn.get_schema()
        cols = {c.name: c for c in schema.tables["orders"].columns}
        assert cols["id"].data_type == "integer"
        assert cols["name"].data_type == "text"
        assert cols["score"].data_type == "double precision"
        assert cols["tags"].data_type == "array"

    def test_required_fields_not_nullable(self, monkeypatch):
        conn, _ = _connector_with_schema(monkeypatch, JSON_SCHEMA)
        schema = conn.get_schema()
        cols = {c.name: c for c in schema.tables["orders"].columns}
        assert cols["id"].is_nullable is False
        assert cols["score"].is_nullable is True
        assert cols["nickname"].is_nullable is True


class TestKafkaErrors:
    def test_registry_failure_raises_runtime_error(self, monkeypatch):
        conn = KafkaConnector("b:9092", schema_registry_url="http://r", topic="orders")
        fake_client = MagicMock()
        fake_client.get_latest_version.side_effect = Exception("connection refused")
        monkeypatch.setattr(conn, "_registry_client", lambda: fake_client)
        with pytest.raises(RuntimeError, match="Failed to fetch schema"):
            conn.get_schema()

    def test_open_session_not_supported(self):
        conn = KafkaConnector("b:9092", schema_registry_url="http://r", topic="orders")
        with pytest.raises(NotImplementedError, match="kafka-start"):
            conn.open_session()
