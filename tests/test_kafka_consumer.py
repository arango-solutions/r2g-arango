"""Tests for the Kafka CDC consumer."""

from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from r2g.cdc.handler import CDCHandler
from r2g.cdc.models import ChangeOperation
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
    w.insert_document = MagicMock(return_value={"_key": "1"})
    w.replace_document = MagicMock(return_value={"_key": "1"})
    w.delete_document = MagicMock(return_value=True)
    w.ensure_collection = MagicMock()
    w.apply_delta = MagicMock()
    mock_db = MagicMock()
    type(w).db = PropertyMock(return_value=mock_db)
    return w


@pytest.fixture
def handler(mock_writer, schema, config):
    return CDCHandler(mock_writer, schema, config)


def _make_debezium_msg(op, table, after=None, before=None, lsn=None, tx_id=None):
    """Build a mock Kafka message with Debezium payload."""
    payload = {
        "before": before,
        "after": after,
        "source": {"schema": "public", "table": table},
        "op": op,
    }
    if lsn is not None:
        payload["source"]["lsn"] = lsn
    if tx_id is not None:
        payload["source"]["txId"] = tx_id

    msg = MagicMock()
    msg.value.return_value = json.dumps(payload).encode("utf-8")
    msg.error.return_value = None
    msg.topic.return_value = f"dbserver1.public.{table}"
    msg.partition.return_value = 0
    msg.offset.return_value = 0
    return msg


@pytest.fixture
def mock_confluent_kafka():
    """Mock the confluent_kafka module."""
    mock_module = MagicMock()
    mock_consumer_cls = MagicMock()
    mock_module.Consumer = mock_consumer_cls
    mock_module.KafkaError = MagicMock()
    mock_module.KafkaError._PARTITION_EOF = -191

    with patch.dict(sys.modules, {"confluent_kafka": mock_module}):
        yield mock_module, mock_consumer_cls


class TestKafkaConsumerInit:
    def test_valid_format(self, handler, mock_confluent_kafka):
        from r2g.cdc.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            handler=handler,
            brokers="localhost:9092",
            topics=["test.public.users"],
            message_format="debezium",
        )
        assert consumer.message_format == "debezium"
        assert consumer.topics == ["test.public.users"]

    def test_flat_format(self, handler, mock_confluent_kafka):
        from r2g.cdc.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            handler=handler,
            topics=["changes"],
            message_format="flat",
        )
        assert consumer.message_format == "flat"

    def test_invalid_format(self, handler, mock_confluent_kafka):
        from r2g.cdc.kafka_consumer import KafkaConsumer

        with pytest.raises(ValueError, match="Unsupported message format"):
            KafkaConsumer(handler=handler, message_format="avro")


class TestKafkaConsumerBuildConfig:
    def test_default_config(self, handler, mock_confluent_kafka):
        from r2g.cdc.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(handler=handler, topics=["t"])
        conf = consumer._build_config()
        assert conf["bootstrap.servers"] == "localhost:9092"
        assert conf["group.id"] == "r2g-cdc"
        assert conf["auto.offset.reset"] == "earliest"
        assert conf["enable.auto.commit"] is False

    def test_extra_config(self, handler, mock_confluent_kafka):
        from r2g.cdc.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(
            handler=handler,
            topics=["t"],
            extra_config={"security.protocol": "SASL_SSL"},
        )
        conf = consumer._build_config()
        assert conf["security.protocol"] == "SASL_SSL"


class TestKafkaConsumerParseMessage:
    def test_parse_debezium_message(self, handler, mock_confluent_kafka):
        from r2g.cdc.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(handler=handler, topics=["t"])
        msg = _make_debezium_msg("c", "users", after={"id": 1, "name": "Alice"})
        evt = consumer._parse_message(msg)
        assert evt is not None
        assert evt.operation == ChangeOperation.INSERT
        assert evt.table_name == "users"

    def test_null_value_returns_none(self, handler, mock_confluent_kafka):
        from r2g.cdc.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(handler=handler, topics=["t"])
        msg = MagicMock()
        msg.value.return_value = None
        assert consumer._parse_message(msg) is None


class TestKafkaConsumerRun:
    def test_consumes_and_processes(self, handler, mock_confluent_kafka):
        from r2g.cdc.kafka_consumer import KafkaConsumer

        _, mock_consumer_cls = mock_confluent_kafka
        mock_ck = MagicMock()
        mock_consumer_cls.return_value = mock_ck

        msgs = [
            _make_debezium_msg("c", "users", after={"id": 1, "name": "Alice"}, lsn=100),
            _make_debezium_msg("c", "users", after={"id": 2, "name": "Bob"}, lsn=101),
        ]

        call_count = 0

        def consume_side_effect(num_messages=500, timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return msgs
            return []

        mock_ck.consume.side_effect = consume_side_effect

        consumer = KafkaConsumer(handler=handler, topics=["test.public.users"])

        def stop_after_first(*args, **kwargs):
            consumer.stop()

        mock_ck.commit = MagicMock(side_effect=stop_after_first)

        consumer.run()

        assert handler.stats.events_received == 2
        assert handler.stats.deltas_applied == 2
        mock_ck.commit.assert_called_once()
        mock_ck.close.assert_called_once()

    def test_handles_error_messages(self, handler, mock_confluent_kafka):
        from r2g.cdc.kafka_consumer import KafkaConsumer

        _, mock_consumer_cls = mock_confluent_kafka
        mock_ck = MagicMock()
        mock_consumer_cls.return_value = mock_ck

        error_msg = MagicMock()
        error_obj = MagicMock()
        error_obj.code.return_value = -191  # _PARTITION_EOF
        error_msg.error.return_value = error_obj
        error_msg.topic.return_value = "test"
        error_msg.partition.return_value = 0
        error_msg.offset.return_value = 0

        good_msg = _make_debezium_msg("c", "users", after={"id": 1})

        call_count = 0

        def consume_side_effect(num_messages=500, timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [error_msg, good_msg]
            return []

        mock_ck.consume.side_effect = consume_side_effect

        consumer = KafkaConsumer(handler=handler, topics=["test"])

        def stop_after_first(*args, **kwargs):
            consumer.stop()

        mock_ck.commit = MagicMock(side_effect=stop_after_first)

        consumer.run()

        assert handler.stats.events_received == 1


class TestKafkaConsumerStop:
    def test_stop_sets_flag(self, handler, mock_confluent_kafka):
        from r2g.cdc.kafka_consumer import KafkaConsumer

        consumer = KafkaConsumer(handler=handler, topics=["t"])
        assert consumer._running is False
        consumer._running = True
        consumer.stop()
        assert consumer._running is False
