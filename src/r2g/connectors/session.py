"""Source-agnostic session abstraction for reading bulk data.

The schema-introspection slice (P6.1 / P6.5) only required a one-shot
``get_schema`` call. Phase 6 slice 3 (P6.3 dump export + P6.4 streaming)
needs a *longer-lived* lifecycle: open a consistent snapshot, count
rows, iterate rows in batches, optionally export to CSV, then close.
Both PostgreSQL and Snowflake support this, but with different
idioms — so we put the idioms behind a single :class:`SourceSession`
Protocol that :class:`StreamingPipeline` and the CLI can share.

Design notes
------------

- **Consistent-snapshot semantics.** PG uses an explicit
  ``REPEATABLE READ`` transaction. Snowflake uses any multi-statement
  transaction (``BEGIN`` … ``COMMIT``) which implicitly sees a
  consistent snapshot for its lifetime. Both are achieved by opening a
  session *once* and reusing it across every table read in a given
  pipeline pass.
- **Parallel workers** each open *their own* session. That gives each
  worker its own snapshot, which is consistent with today's PG
  behaviour (each worker already calls ``psycopg.connect`` in the
  worker body) and is the natural Snowflake idiom (one connection per
  warehouse slot).
- **`stream_rows` returns dicts**, keyed by column name, matching
  what ``NodeTransformer`` / ``EdgeTransformer`` already expect. PG
  uses ``psycopg.rows.dict_row``; Snowflake zips
  ``cursor.description`` with each row tuple.
- **`dump_table_to_csv` returns the row count.** PG is free to use
  ``COPY TO STDOUT`` (fast path); Snowflake writes through Python's
  ``csv`` module since client-side ``COPY INTO @stage`` would require
  provisioning a stage that we don't own. Semantics stay identical
  (header row, empty string for NULL).
- The Protocol is structural, so existing test doubles that provide
  the same methods satisfy it without inheritance.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, Optional, Protocol, runtime_checkable


@runtime_checkable
class SourceSession(Protocol):
    """A live, bulk-reading session over a relational source.

    Implementations own a database connection/cursor with
    consistent-snapshot semantics for the duration of the session.
    """

    def count_rows(
        self,
        table: str,
        *,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> int:
        """Count rows in *table*, optionally filtered by ``since_column >= since_value``."""
        ...

    def stream_rows(
        self,
        table: str,
        *,
        batch_size: int = 10_000,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield one ``dict`` per row, in insertion order where the source preserves it.

        Implementations should use a server-side cursor / bounded
        fetch so memory stays proportional to ``batch_size`` regardless
        of table size.
        """
        ...

    def dump_table_to_csv(
        self,
        table: str,
        out_path: Path,
        *,
        header: bool = True,
    ) -> int:
        """Write *table* to *out_path* as CSV, returning the row count.

        The output contract is deliberately simple: comma-separated,
        empty string for NULL, RFC-4180-style double-quoting. A header
        row is emitted when ``header=True``.
        """
        ...

    def close(self) -> None:
        """Release the underlying connection / transaction."""
        ...


__all__ = ["SourceSession"]
