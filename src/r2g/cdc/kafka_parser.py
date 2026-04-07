"""Debezium / generic Kafka message parsers for CDC events.

Converts Kafka message payloads (JSON) into ChangeEvent objects that
feed into the existing CDCHandler pipeline.

Supported formats:

- **Debezium JSON envelope** (default) -- the standard Debezium
  ``before``/``after``/``op`` envelope with embedded ``source`` metadata.
  Works with Debezium connectors for PostgreSQL (pgoutput/decoderbufs),
  MySQL, SQL Server, etc.

- **Flat JSON** -- simple ``{"operation":"INSERT", "table":"...",
  "new_row":{...}}`` messages for custom producers that match our
  ChangeEvent schema directly.
"""

from __future__ import annotations

import json
from typing import Any

from r2g.cdc.models import ChangeEvent, ChangeOperation
from r2g.log import get_logger

logger = get_logger(__name__)

_DEBEZIUM_OP_MAP = {
    "c": ChangeOperation.INSERT,
    "r": ChangeOperation.INSERT,  # snapshot read → treat as insert
    "u": ChangeOperation.UPDATE,
    "d": ChangeOperation.DELETE,
}

_FLAT_OP_MAP = {
    "INSERT": ChangeOperation.INSERT,
    "UPDATE": ChangeOperation.UPDATE,
    "DELETE": ChangeOperation.DELETE,
}


class DebeziumParser:
    """Parse Debezium JSON envelope messages into ChangeEvents.

    Debezium envelope structure::

        {
          "before": {...} | null,
          "after":  {...} | null,
          "source": {
            "schema": "public",
            "table": "users",
            "lsn": 12345,
            "txId": 42,
            ...
          },
          "op": "c" | "u" | "d" | "r",
          "ts_ms": 1700000000000
        }

    ``op`` values: ``c`` = create, ``u`` = update, ``d`` = delete,
    ``r`` = snapshot read (treated as insert).

    If the message arrives inside a Kafka Connect wrapper with a
    ``payload`` key, the parser unwraps it automatically.
    """

    def __init__(self, default_schema: str = "public") -> None:
        self._default_schema = default_schema

    def parse(self, raw: str | bytes | dict[str, Any]) -> ChangeEvent | None:
        """Parse a single Debezium message.  Returns None for unrecognised messages."""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if isinstance(raw, str):
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("debezium_invalid_json", data=str(raw)[:120])
                return None
        else:
            payload = raw

        if "payload" in payload and isinstance(payload["payload"], dict):
            payload = payload["payload"]

        op_code = payload.get("op")
        if op_code is None:
            logger.debug("debezium_no_op_field", keys=list(payload.keys()))
            return None

        operation = _DEBEZIUM_OP_MAP.get(op_code)
        if operation is None:
            logger.warning("debezium_unknown_op", op=op_code)
            return None

        source = payload.get("source", {})
        schema_name = source.get("schema", self._default_schema)
        table_name = source.get("table", "")
        if not table_name:
            logger.warning("debezium_no_table", source=source)
            return None

        lsn_raw = source.get("lsn")
        lsn = str(lsn_raw) if lsn_raw is not None else None
        tx_id = source.get("txId")

        before = payload.get("before")
        after = payload.get("after")

        return ChangeEvent(
            operation=operation,
            schema_name=schema_name,
            table_name=table_name,
            new_row=after if after else None,
            old_row=before if before else None,
            lsn=lsn,
            transaction_id=tx_id,
        )

    def parse_batch(
        self, messages: list[str | bytes | dict[str, Any]]
    ) -> list[ChangeEvent]:
        """Parse multiple messages, skipping unrecognised ones."""
        events = []
        for msg in messages:
            evt = self.parse(msg)
            if evt is not None:
                events.append(evt)
        return events


class FlatJsonParser:
    """Parse simple flat JSON messages matching our ChangeEvent schema.

    Expected format::

        {
          "operation": "INSERT",
          "schema_name": "public",
          "table_name": "users",
          "new_row": {"id": 1, "name": "Alice"},
          "old_row": null,
          "lsn": "0/ABC",
          "transaction_id": 42
        }
    """

    def parse(self, raw: str | bytes | dict[str, Any]) -> ChangeEvent | None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if isinstance(raw, str):
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("flat_json_invalid", data=str(raw)[:120])
                return None
        else:
            payload = raw

        op_str = payload.get("operation", "")
        operation = _FLAT_OP_MAP.get(op_str.upper())
        if operation is None:
            logger.debug("flat_json_unknown_op", op=op_str)
            return None

        table = payload.get("table_name", "")
        if not table:
            return None

        return ChangeEvent(
            operation=operation,
            schema_name=payload.get("schema_name", "public"),
            table_name=table,
            new_row=payload.get("new_row"),
            old_row=payload.get("old_row"),
            lsn=payload.get("lsn"),
            transaction_id=payload.get("transaction_id"),
        )
