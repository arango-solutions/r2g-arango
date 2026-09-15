"""``postgres`` — the S1 walking-skeleton dialect (PLAN F-3/F-4).

Declared PK/FK constraints, one canonical roundtrip-stable type per JSON type,
one ``INSERT`` per row. Output is byte-compatible with the skeleton.
"""

from __future__ import annotations

from typing import ClassVar, Dict

from .base import SqlDialect

#: The one canonical Postgres spelling per JSON type (PLAN F-3): the generated
#: DDL type must introspect back through ``config.pg_type_to_json_type`` to the
#: same JSON type.
PG_TYPE_FOR_JSON_TYPE: Dict[str, str] = {
    "integer": "bigint",
    "float": "double precision",
    "boolean": "boolean",
    "string": "text",
}


class PostgresDialect(SqlDialect):
    name: ClassVar[str] = "postgres"
    type_for_json: ClassVar[Dict[str, str]] = PG_TYPE_FOR_JSON_TYPE
    multi_row_insert: ClassVar[bool] = False


__all__ = ["PG_TYPE_FOR_JSON_TYPE", "PostgresDialect"]
