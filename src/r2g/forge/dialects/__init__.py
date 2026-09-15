"""The dialect registry (ADR-0006 D-4: "new dialects arrive as plugins behind
one seam").

Adding a dialect is three steps: write ``dialects/<name>.py`` following
:mod:`.base`, import it here, and add one instance to :data:`DIALECTS`.
Nothing else in the forge knows dialect names.
"""

from __future__ import annotations

from typing import Dict, Tuple

from ..core import ForgeError
from .arango import ArangoDialect
from .base import ForgeDialect, SqlDialect, check_type_table, split_sql_statements, sql_literal
from .clickhouse import ClickHouseDialect
from .postgres import PostgresDialect
from .snowflake import SnowflakeDialect

DIALECTS: Dict[str, ForgeDialect] = {
    d.name: d
    for d in (
        PostgresDialect(),
        SnowflakeDialect(),
        ClickHouseDialect(),
        ArangoDialect(),
    )
}
for _dialect in DIALECTS.values():
    check_type_table(_dialect)

#: Dialects the seam accepts, in registration order (Postgres first: the S1
#: skeleton and the CLI default).
SUPPORTED_DIALECTS: Tuple[str, ...] = tuple(DIALECTS)


def get_dialect(name: str) -> ForgeDialect:
    """Look a dialect up by registry key; unknown names are a
    :class:`~r2g.forge.core.ForgeError` (a caller problem, exit 2 in the CLI)."""
    try:
        return DIALECTS[name]
    except KeyError:
        raise ForgeError(
            f"dialect {name!r} is not supported (supported: {list(SUPPORTED_DIALECTS)})"
        ) from None


__all__ = [
    "DIALECTS",
    "SUPPORTED_DIALECTS",
    "ArangoDialect",
    "ClickHouseDialect",
    "ForgeDialect",
    "PostgresDialect",
    "SnowflakeDialect",
    "SqlDialect",
    "check_type_table",
    "get_dialect",
    "split_sql_statements",
    "sql_literal",
]
