"""Kafka topic source connector (introspection-only).

A Kafka source is a topic whose message schema is registered in a
Confluent Schema Registry. Introspection fetches the latest schema for
the topic's value subject (``{topic}-value`` by default) and converts
its fields into a single :class:`~r2g.types.Table` named after the
topic. Both Avro record schemas and JSON Schema documents are
supported.

Streaming Kafka into ArangoDB is handled by the existing Kafka CDC
worker (``r2g kafka-start``); :meth:`KafkaConnector.open_session` is
therefore intentionally unsupported here — this connector exists so the
catalog and Mapping Studio can *see* a topic's shape, not to drive a
batch load through :class:`~r2g.streaming.pipeline.StreamingPipeline`.

``confluent-kafka`` is an optional dependency; a missing driver raises
:class:`ImportError` with a ``pip install 'r2g-arango[kafka]'`` hint.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from r2g.log import get_logger
from r2g.types import Column, Schema, Table

logger = get_logger(__name__)

# Avro primitive / logical type -> source-agnostic type string.
_AVRO_TYPE_MAP: dict[str, str] = {
    "int": "integer",
    "long": "integer",
    "float": "double precision",
    "double": "double precision",
    "boolean": "boolean",
    "string": "text",
    "bytes": "text",
    "enum": "text",
    "array": "array",
    "map": "object",
    "record": "object",
    "fixed": "text",
    "null": "text",
}

# Avro logical types refine an underlying primitive.
_AVRO_LOGICAL_MAP: dict[str, str] = {
    "date": "date",
    "time-millis": "text",
    "time-micros": "text",
    "timestamp-millis": "timestamp",
    "timestamp-micros": "timestamp",
    "decimal": "double precision",
    "uuid": "text",
}

# JSON Schema type -> source-agnostic type string.
_JSON_SCHEMA_TYPE_MAP: dict[str, str] = {
    "integer": "integer",
    "number": "double precision",
    "boolean": "boolean",
    "string": "text",
    "array": "array",
    "object": "object",
    "null": "text",
}


class KafkaConnector:
    """Introspect a Kafka topic's value schema via the Schema Registry.

    ``connection_string`` is the bootstrap servers list (kept for parity
    / future streaming use). The Schema Registry URL and topic come from
    ``source_params`` at the factory and are passed explicitly here.
    """

    def __init__(
        self,
        connection_string: str,
        *,
        schema_registry_url: str,
        topic: str,
        subject: Optional[str] = None,
        schema_name: str = "public",
    ) -> None:
        if not schema_registry_url:
            raise ValueError("Kafka source requires a 'schema_registry_url' parameter")
        if not topic:
            raise ValueError("Kafka source requires a 'topic' parameter")
        self.connection_string = connection_string
        self.schema_name = schema_name
        self.schema_registry_url = schema_registry_url
        self.topic = topic
        self.subject = subject or f"{topic}-value"

    def _registry_client(self) -> Any:
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient
        except ImportError as e:  # pragma: no cover - exercised via tests with monkeypatch
            raise ImportError(
                "confluent-kafka is required for Kafka sources. "
                "Install with: pip install 'r2g-arango[kafka]'"
            ) from e
        return SchemaRegistryClient({"url": self.schema_registry_url})

    def get_schema(self) -> Schema:
        client = self._registry_client()
        try:
            registered = client.get_latest_version(self.subject)
            schema_str = registered.schema.schema_str
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                f"Failed to fetch schema for subject '{self.subject}' "
                f"from {self.schema_registry_url}: {e}"
            )

        columns = self._parse_schema(schema_str)
        table = Table(name=self.topic, columns=columns, primary_key=[], foreign_keys=[])
        return Schema(tables={self.topic: table})

    def open_session(self) -> Any:
        raise NotImplementedError(
            "Batch loading from Kafka is not supported via the streaming pipeline. "
            "Use 'r2g kafka-start' for continuous Kafka -> ArangoDB sync; this "
            "connector only introspects the topic schema."
        )

    @staticmethod
    def _parse_schema(schema_str: str) -> list[Column]:
        try:
            doc = json.loads(schema_str)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Schema is not valid JSON: {e}")

        if isinstance(doc, dict) and doc.get("type") == "record" and "fields" in doc:
            return KafkaConnector._parse_avro_record(doc)
        if isinstance(doc, dict) and ("properties" in doc or doc.get("type") == "object"):
            return KafkaConnector._parse_json_schema(doc)
        # Fallback: a bare primitive schema -> single 'value' column.
        return [Column(name="value", data_type="text", is_nullable=True)]

    @staticmethod
    def _parse_avro_record(doc: dict[str, Any]) -> list[Column]:
        columns: list[Column] = []
        for field in doc.get("fields", []):
            name = field.get("name")
            if not name:
                continue
            data_type, nullable = KafkaConnector._resolve_avro_type(field.get("type"))
            columns.append(Column(name=name, data_type=data_type, is_nullable=nullable))
        return columns

    @staticmethod
    def _resolve_avro_type(avro_type: Any) -> tuple[str, bool]:
        """Return (type_string, is_nullable) for an Avro field type.

        Handles unions (``["null", "string"]``), logical types
        (``{"type": "int", "logicalType": "date"}``), and nested
        record/array/map shapes (reported as object/array).
        """
        nullable = False
        if isinstance(avro_type, list):
            non_null = [t for t in avro_type if t != "null"]
            nullable = len(non_null) != len(avro_type)
            avro_type = non_null[0] if non_null else "null"

        if isinstance(avro_type, dict):
            logical = avro_type.get("logicalType")
            if logical and logical in _AVRO_LOGICAL_MAP:
                return _AVRO_LOGICAL_MAP[logical], nullable
            inner = avro_type.get("type", "string")
            return _AVRO_TYPE_MAP.get(inner, "text"), nullable

        if isinstance(avro_type, str):
            return _AVRO_TYPE_MAP.get(avro_type, "text"), nullable

        return "text", nullable

    @staticmethod
    def _parse_json_schema(doc: dict[str, Any]) -> list[Column]:
        required = set(doc.get("required", []))
        columns: list[Column] = []
        for name, spec in (doc.get("properties") or {}).items():
            json_type = spec.get("type") if isinstance(spec, dict) else None
            if isinstance(json_type, list):
                non_null = [t for t in json_type if t != "null"]
                json_type = non_null[0] if non_null else "null"
            data_type = _JSON_SCHEMA_TYPE_MAP.get(json_type or "string", "text")
            columns.append(
                Column(
                    name=name,
                    data_type=data_type,
                    is_nullable=name not in required,
                )
            )
        return columns


__all__ = ["KafkaConnector"]
