"""CSV directory source connector.

A CSV source is a directory of delimited files: each file is one table,
the filename stem is the table name, and the header row supplies the
column names. Column types are inferred from a bounded sample using
Polars' CSV type inference, then mapped onto the same source-agnostic
type strings (``integer`` / ``double precision`` / ``boolean`` /
``timestamp`` / ``text``) that the transformer's type map already
understands.

CSV has no notion of primary or foreign keys. We apply one light,
documented heuristic so auto-mapping produces a usable ``_key``: a
column named exactly ``id`` (case-insensitive) is treated as the
primary key. Relationships can be recovered afterwards with FK
inference (``r2g source infer-fks``).

Both schema introspection (:meth:`CsvConnector.get_schema`) and bulk
reads for loads (:class:`CsvSession`) are supported, so a CSV source
can be streamed into ArangoDB exactly like a relational source.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Iterator, Optional

import polars as pl

from r2g.input.dump_reader import DumpReader
from r2g.log import get_logger
from r2g.types import Column, Schema, Table

logger = get_logger(__name__)

# Polars dtype "base name" -> source-agnostic type string understood by
# r2g.config.DEFAULT_TYPE_MAP / pg_type_to_json_type.
_POLARS_TYPE_MAP: dict[str, str] = {
    "Int8": "integer",
    "Int16": "integer",
    "Int32": "integer",
    "Int64": "integer",
    "UInt8": "integer",
    "UInt16": "integer",
    "UInt32": "integer",
    "UInt64": "integer",
    "Float32": "double precision",
    "Float64": "double precision",
    "Boolean": "boolean",
    "Date": "date",
    "Datetime": "timestamp",
    "Time": "text",
    "Utf8": "text",
    "String": "text",
}

_CSV_EXTENSIONS = (".csv", ".tsv", ".txt")


def _polars_dtype_to_type(dtype: Any) -> str:
    """Map a Polars dtype to a source-agnostic type string."""
    base = str(dtype).split("(")[0].strip()
    return _POLARS_TYPE_MAP.get(base, "text")


class CsvConnector:
    """Introspect and read a directory of CSV files as a schema.

    ``connection_string`` is the directory path. ``schema_name`` is
    carried for protocol parity but is otherwise unused (a CSV directory
    has no schema namespace).
    """

    def __init__(
        self,
        connection_string: str,
        *,
        schema_name: str = "public",
        delimiter: str = ",",
        has_header: bool = True,
        sample_rows: int = 1000,
    ) -> None:
        self.connection_string = connection_string
        self.schema_name = schema_name
        self.delimiter = delimiter
        self.has_header = has_header
        self.sample_rows = sample_rows
        self.directory = Path(connection_string).expanduser()

    def _table_files(self) -> dict[str, Path]:
        if not self.directory.is_dir():
            raise RuntimeError(f"CSV source directory not found: {self.directory}")
        files: dict[str, Path] = {}
        for path in sorted(self.directory.iterdir()):
            if path.is_file() and path.suffix.lower() in _CSV_EXTENSIONS:
                files[path.stem] = path
        if not files:
            raise RuntimeError(
                f"No CSV/TSV files found in {self.directory} "
                f"(looked for {', '.join(_CSV_EXTENSIONS)})"
            )
        return files

    def get_schema(self) -> Schema:
        schema = Schema()
        for table_name, path in self._table_files().items():
            schema.tables[table_name] = self._process_file(table_name, path)
        return schema

    def open_session(self) -> "CsvSession":
        return CsvSession(
            self.connection_string,
            schema_name=self.schema_name,
            delimiter=self.delimiter,
            has_header=self.has_header,
        )

    def _process_file(self, table_name: str, path: Path) -> Table:
        try:
            frame = pl.read_csv(
                str(path),
                separator=self.delimiter,
                has_header=self.has_header,
                n_rows=self.sample_rows,
                infer_schema_length=self.sample_rows,
            )
        except Exception as e:  # noqa: BLE001
            logger.error("csv_introspect_failed", path=str(path), error=str(e))
            raise RuntimeError(f"Failed to read CSV header for '{table_name}': {e}")

        columns: list[Column] = []
        pk: list[str] = []
        for col_name, dtype in zip(frame.columns, frame.dtypes):
            is_pk = col_name.lower() == "id"
            if is_pk:
                pk.append(col_name)
            columns.append(
                Column(
                    name=col_name,
                    data_type=_polars_dtype_to_type(dtype),
                    is_nullable=not is_pk,
                    is_primary_key=is_pk,
                )
            )

        return Table(name=table_name, columns=columns, primary_key=pk, foreign_keys=[])


class CsvSession:
    """Bulk-read session over a CSV directory.

    There is no live connection to manage; each table read opens its
    file lazily via :class:`DumpReader`. The session object exists to
    satisfy the :class:`~r2g.connectors.session.SourceSession` protocol
    so the streaming pipeline can treat CSV like any other source.
    """

    def __init__(
        self,
        connection_string: str,
        *,
        schema_name: str = "public",
        delimiter: str = ",",
        has_header: bool = True,
    ) -> None:
        self.connection_string = connection_string
        self.schema_name = schema_name
        self.delimiter = delimiter
        self.has_header = has_header
        self.directory = Path(connection_string).expanduser()

    def _resolve(self, table: str) -> Path:
        for ext in _CSV_EXTENSIONS:
            candidate = self.directory / f"{table}{ext}"
            if candidate.is_file():
                return candidate
        raise RuntimeError(f"No CSV file for table '{table}' in {self.directory}")

    def _reader(self, table: str) -> DumpReader:
        return DumpReader(
            str(self._resolve(table)),
            delimiter=self.delimiter,
            has_header=self.has_header,
        )

    def count_rows(
        self,
        table: str,
        *,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> int:
        return self._reader(table).row_count()

    def stream_rows(
        self,
        table: str,
        *,
        batch_size: int = 10_000,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]:
        yield from self._reader(table).read_rows()

    def dump_table_to_csv(
        self,
        table: str,
        out_path: Path,
        *,
        header: bool = True,
    ) -> int:
        src = self._resolve(table)
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, out_path)
        return self._reader(table).row_count()

    def close(self) -> None:
        return None

    def __enter__(self) -> "CsvSession":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


__all__ = ["CsvConnector", "CsvSession"]
