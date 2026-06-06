"""Abstraction over relational source connectors.

R2G started as a PostgreSQL-only tool; `PostgresConnector` was the single
concrete implementation with a hand-rolled constructor signature. PRD
Phase 6 (Snowflake) calls out an explicit "source abstraction layer"
(P6.5) so the schema-reader, dump, and streaming paths can accept other
relational sources behind a common interface.

This module intentionally stays minimal:

- `SourceConnector` is a structural `Protocol` describing the
  operations we actually perform on a connector today: introspect
  `connection_string` / `schema_name` attributes, return a populated
  `Schema` from :meth:`get_schema`, and (Phase 6 slice 3) open a
  :class:`SourceSession` for bulk reads via :meth:`open_session`.
  `PostgresConnector` and `SnowflakeConnector` both satisfy the
  protocol.
- `SUPPORTED_SOURCE_TYPES` is the registry of source types the catalog,
  UI, and MCP server are allowed to create. Adding a new type is a
  single edit here plus a concrete implementation.
- `create_source_connector` is the thin factory the UI / MCP /
  `source snapshot` / `stream` / `source dump` commands call. It
  lazy-imports the concrete class so optional dependencies (e.g.
  ``snowflake-connector-python``) are only loaded when the user
  actually has a source of that type.

As of Phase 6 slice 3 the streaming pipeline and dump-tables CLI both
consume connectors through this protocol; the PG-only fast paths live
inside ``PostgresSession`` and the Snowflake equivalents inside
``SnowflakeSession``.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from r2g.connectors.session import SourceSession
from r2g.types import Schema


@runtime_checkable
class SourceConnector(Protocol):
    """Structural interface every R2G source connector must satisfy."""

    connection_string: str
    schema_name: str

    def get_schema(self) -> Schema:
        """Introspect the upstream source and return a populated :class:`Schema`."""
        ...

    def open_session(self) -> SourceSession:
        """Open a bulk-read session with consistent-snapshot semantics.

        Each call returns a fresh session; callers are expected to call
        :meth:`SourceSession.close` when done. Parallel workers each
        obtain their own session so they do not contend for a single
        cursor.
        """
        ...


SUPPORTED_SOURCE_TYPES: tuple[str, ...] = ("postgresql", "snowflake", "csv", "kafka")

# Aliases that all mean PostgreSQL. Missing/empty source types default to PG.
_PG_ALIASES: frozenset[str] = frozenset({"postgresql", "postgres", "pg"})


def normalize_source_type(source_type: str | None) -> str:
    """Canonicalize a source-type string.

    Empty/``None`` defaults to ``"postgresql"`` (R2G's historical default) and
    the ``postgres`` / ``pg`` aliases fold to ``"postgresql"``.
    """
    key = (source_type or "postgresql").strip().lower()
    return "postgresql" if key in _PG_ALIASES else key


def is_postgresql(source_type: str | None) -> bool:
    """True when ``source_type`` denotes PostgreSQL (incl. ``postgres`` / ``pg``)."""
    return normalize_source_type(source_type) == "postgresql"


def serialize_rows(rows: list[dict]) -> list[dict]:
    """Convert non-JSON-serializable DB values to JSON-safe forms.

    ``datetime``/``date`` → ISO string, ``Decimal`` → float, ``bytes`` → hex.
    Used by the data-preview paths in the UI and MCP servers.
    """
    import datetime as dt
    from decimal import Decimal

    result = []
    for row in rows:
        converted = {}
        for k, v in row.items():
            if isinstance(v, (dt.datetime, dt.date)):
                converted[k] = v.isoformat()
            elif isinstance(v, Decimal):
                converted[k] = float(v)
            elif isinstance(v, bytes):
                converted[k] = v.hex()
            else:
                converted[k] = v
        result.append(converted)
    return result


def create_source_connector(
    source_type: str,
    connection_string: str,
    schema_name: str = "public",
    *,
    source_params: dict | None = None,
) -> SourceConnector:
    """Return a connector matching ``source_type``.

    Concrete classes are imported lazily so that users who never touch a
    given source type do not pay for its optional dependency. Unknown /
    unsupported types raise :class:`ValueError`; missing optional deps
    raise :class:`ImportError` with a pip-install hint.

    ``source_params`` carries type-specific configuration (e.g. CSV
    ``delimiter`` / ``has_header``, Kafka ``schema_registry_url`` /
    ``topic``). PostgreSQL and Snowflake ignore it.
    """

    key = normalize_source_type(source_type)
    params = source_params or {}
    if key == "postgresql":
        from r2g.connectors.postgres import PostgresConnector

        return PostgresConnector(connection_string, schema_name=schema_name)
    if key == "snowflake":
        from r2g.connectors.snowflake import SnowflakeConnector

        return SnowflakeConnector(connection_string, schema_name=schema_name)
    if key == "csv":
        from r2g.connectors.csv_source import CsvConnector

        return CsvConnector(
            connection_string,
            schema_name=schema_name,
            delimiter=params.get("delimiter", ","),
            has_header=bool(params.get("has_header", True)),
        )
    if key == "kafka":
        from r2g.connectors.kafka_source import KafkaConnector

        return KafkaConnector(
            connection_string,
            schema_registry_url=params.get("schema_registry_url", ""),
            topic=params.get("topic", ""),
            subject=params.get("subject"),
            schema_name=schema_name,
        )
    raise ValueError(
        f"Unsupported source type '{source_type}'. "
        f"Expected one of: {', '.join(SUPPORTED_SOURCE_TYPES)}."
    )
