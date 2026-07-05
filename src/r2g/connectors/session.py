"""Source-agnostic session abstraction for reading bulk data.

This module is a thin re-export of ``relational_schema_analyzer.connectors.session``.
The :class:`SourceSession` Protocol was extracted to ``relational-schema-analyzer``
(RSA) and is byte-identical there, so r2g imports the single shared definition
rather than duplicating it. The ``r2g.connectors.session`` import path is preserved
for existing callers (connectors, the streaming pipeline, and test doubles). See
``docs/internal/DESIGN-rsa-compat-layer.md`` (Stage 2 close-out).

``SourceSession`` is a structural, ``runtime_checkable`` Protocol describing a
live, bulk-reading session over a relational source (``count_rows`` /
``stream_rows`` / ``dump_table_to_csv`` / ``close``) with consistent-snapshot
semantics for the session's lifetime. r2g's concrete sessions
(``PostgresSession``, ``SnowflakeSession``, ``MySQLSession``, ``SQLServerSession``,
``CsvSession``) and test doubles satisfy it by shape.
"""

from __future__ import annotations

from relational_schema_analyzer.connectors.session import SourceSession as SourceSession

__all__ = ["SourceSession"]
