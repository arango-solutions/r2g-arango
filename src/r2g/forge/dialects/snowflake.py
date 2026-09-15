"""``snowflake`` — Snowflake SQL with declared (recorded, unenforced) keys.

Identifiers are emitted **unquoted UPPERCASE** — the spelling real Snowflake
schemas have after identifier folding, and the one r2g's forward pipeline
handles today (``singularize``/``convert_identifier`` are case-insensitive:
``ACCOUNTS`` -> ``Account``, ``ACCOUNT_NAME`` -> ``accountName``). Quoting
lower-case names would also round-trip, but would exercise a spelling no
customer schema has.

Snowflake records ``PRIMARY KEY`` / ``FOREIGN KEY`` constraints without
enforcing them; RSA's ``SnowflakeConnector`` reads them back via
``SHOW PRIMARY KEYS`` / ``SHOW IMPORTED KEYS``, which is exactly the declared
path F-4 wants exercised. ``integer`` is emitted as ``NUMBER(38,0)``: Snowflake
has no other integer type, and ``INFORMATION_SCHEMA`` reports it as ``NUMBER``
(see the roundtrip test's known-gap table for what that costs today).
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

from ..core import SchemaPlan
from .base import SqlDialect

SNOWFLAKE_TYPE_FOR_JSON_TYPE: Dict[str, str] = {
    "integer": "NUMBER(38,0)",
    "float": "FLOAT",
    "boolean": "BOOLEAN",
    "string": "VARCHAR",
}


class SnowflakeDialect(SqlDialect):
    name: ClassVar[str] = "snowflake"
    type_for_json: ClassVar[Dict[str, str]] = SNOWFLAKE_TYPE_FOR_JSON_TYPE

    def physical_table(self, table: str) -> str:
        return table.upper()

    def physical_column(self, column: str) -> str:
        return column.upper()

    def ddl_header(self, plan: SchemaPlan) -> List[str]:
        return [
            "-- Identifiers are unquoted UPPERCASE (Snowflake folding); constraints are",
            "-- recorded by Snowflake but not enforced — which is what the analyzers read.",
        ]


__all__ = ["SNOWFLAKE_TYPE_FOR_JSON_TYPE", "SnowflakeDialect"]
