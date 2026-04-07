"""Output plugin parsers for PostgreSQL logical decoding.

Converts raw text/JSON output from logical replication slots into
ChangeEvent objects.  Two plugins are supported:

- ``test_decoding`` (built-in to PostgreSQL, no extensions needed)
- ``wal2json`` (format-version 1, requires the wal2json extension)
"""

from __future__ import annotations

import json
import re
from typing import Any

from r2g.cdc.models import ChangeEvent, ChangeOperation
from r2g.log import get_logger

logger = get_logger(__name__)

_HEADER_RE = re.compile(
    r"^table\s+(\S+)\.(\S+):\s+(INSERT|UPDATE|DELETE):\s*(.*)",
    re.DOTALL,
)


class TestDecodingParser:
    """Parse ``test_decoding`` textual output into ChangeEvents.

    Expected line formats::

        BEGIN 685
        COMMIT 685
        table public.users: INSERT: id[int4]:1 name[text]:'Alice'
        table public.users: UPDATE: id[int4]:1 name[text]:'Bob'
        table public.users: UPDATE: old-key: id[int4]:1 new-tuple: id[int4]:1 name[text]:'Bob'
        table public.users: DELETE: id[int4]:1

    BEGIN / COMMIT lines are silently skipped (returns ``None``).
    """

    def parse_message(
        self,
        data: str,
        lsn: str | None = None,
        xid: int | None = None,
    ) -> ChangeEvent | None:
        data = data.strip()
        if data.startswith("BEGIN") or data.startswith("COMMIT"):
            return None

        m = _HEADER_RE.match(data)
        if m is None:
            logger.debug("test_decoding_unparseable", data=data[:120])
            return None

        schema, table, op_str, rest = m.groups()
        operation = ChangeOperation(op_str)

        if operation == ChangeOperation.INSERT:
            return ChangeEvent(
                operation=operation,
                schema_name=schema,
                table_name=table,
                new_row=self._parse_columns(rest),
                lsn=lsn,
                transaction_id=xid,
            )

        if operation == ChangeOperation.DELETE:
            return ChangeEvent(
                operation=operation,
                schema_name=schema,
                table_name=table,
                old_row=self._parse_columns(rest),
                lsn=lsn,
                transaction_id=xid,
            )

        old_row, new_row = self._parse_update_columns(rest)
        return ChangeEvent(
            operation=operation,
            schema_name=schema,
            table_name=table,
            old_row=old_row,
            new_row=new_row,
            lsn=lsn,
            transaction_id=xid,
        )

    # ------------------------------------------------------------------
    # UPDATE helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_update_columns(
        rest: str,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        if "old-key:" in rest and "new-tuple:" in rest:
            idx = rest.index("new-tuple:")
            old_part = rest[: idx].replace("old-key:", "", 1).strip()
            new_part = rest[idx + len("new-tuple:") :].strip()
            return (
                TestDecodingParser._parse_columns(old_part),
                TestDecodingParser._parse_columns(new_part),
            )
        if "old-key:" in rest:
            old_part = rest.replace("old-key:", "", 1).strip()
            return TestDecodingParser._parse_columns(old_part), None
        return None, TestDecodingParser._parse_columns(rest)

    # ------------------------------------------------------------------
    # Column‑list parser  (left-to-right state machine)
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_columns(text: str) -> dict[str, Any]:
        """Parse ``col[type]:value col[type]:value …`` into a dict."""
        result: dict[str, Any] = {}
        pos = 0
        n = len(text)

        while pos < n:
            while pos < n and text[pos] == " ":
                pos += 1
            if pos >= n:
                break

            bracket = text.find("[", pos)
            if bracket == -1:
                break
            col_name = text[pos:bracket]

            close = text.find("]", bracket)
            if close == -1:
                break

            pos = close + 1
            if pos < n and text[pos] == ":":
                pos += 1

            value, pos = TestDecodingParser._read_value(text, pos)
            result[col_name] = value

        return result

    @staticmethod
    def _read_value(text: str, pos: int) -> tuple[Any, int]:
        n = len(text)
        if pos >= n:
            return None, pos

        if text[pos] == "'":
            return TestDecodingParser._read_quoted(text, pos)

        end = pos
        while end < n and text[end] != " ":
            end += 1
        token = text[pos:end]

        if token == "null":
            return None, end
        if token == "true":
            return True, end
        if token == "false":
            return False, end
        try:
            return int(token) if "." not in token else float(token), end
        except ValueError:
            return token, end

    @staticmethod
    def _read_quoted(text: str, pos: int) -> tuple[str, int]:
        pos += 1  # skip opening '
        parts: list[str] = []
        n = len(text)
        while pos < n:
            ch = text[pos]
            if ch == "'":
                if pos + 1 < n and text[pos + 1] == "'":
                    parts.append("'")
                    pos += 2
                else:
                    pos += 1
                    break
            else:
                parts.append(ch)
                pos += 1
        return "".join(parts), pos


# ======================================================================
# wal2json format-version 1
# ======================================================================

_KIND_MAP = {
    "insert": ChangeOperation.INSERT,
    "update": ChangeOperation.UPDATE,
    "delete": ChangeOperation.DELETE,
}


class Wal2JsonParser:
    """Parse ``wal2json`` (format-version 1) JSON output.

    Each call to ``pg_logical_slot_get_changes`` returns one JSON object
    per transaction with a ``change`` array.  Example::

        {
          "xid": 42,
          "change": [
            {
              "kind": "insert",
              "schema": "public",
              "table": "users",
              "columnnames": ["id", "name"],
              "columntypes": ["integer", "text"],
              "columnvalues": [1, "Alice"]
            }
          ]
        }

    For UPDATE/DELETE, old key values appear under ``oldkeys``::

        "oldkeys": {
          "keynames": ["id"],
          "keytypes": ["integer"],
          "keyvalues": [1]
        }
    """

    def parse_message(
        self,
        data: str,
        lsn: str | None = None,
    ) -> list[ChangeEvent]:
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            logger.warning("wal2json_invalid_json", data=data[:120])
            return []

        changes = payload.get("change", [])
        xid = payload.get("xid")
        events: list[ChangeEvent] = []

        for change in changes:
            kind = change.get("kind", "")
            op = _KIND_MAP.get(kind)
            if op is None:
                continue

            schema = change.get("schema", "public")
            table = change.get("table", "")

            new_row: dict[str, Any] | None = None
            old_row: dict[str, Any] | None = None

            if op in (ChangeOperation.INSERT, ChangeOperation.UPDATE):
                names = change.get("columnnames", [])
                values = change.get("columnvalues", [])
                new_row = dict(zip(names, values))

            if op in (ChangeOperation.UPDATE, ChangeOperation.DELETE):
                old_keys = change.get("oldkeys", {})
                old_names = old_keys.get("keynames", [])
                old_values = old_keys.get("keyvalues", [])
                if old_names:
                    old_row = dict(zip(old_names, old_values))

            events.append(
                ChangeEvent(
                    operation=op,
                    schema_name=schema,
                    table_name=table,
                    new_row=new_row,
                    old_row=old_row,
                    lsn=lsn,
                    transaction_id=xid,
                )
            )

        return events
