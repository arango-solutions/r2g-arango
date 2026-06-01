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

    key = (source_type or "").strip().lower()
    params = source_params or {}
    if key in ("postgresql", "postgres", "pg"):
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
