"""The dialect seam: how a :class:`~r2g.forge.core.SchemaPlan` and its rows are
projected onto one target system (ADR-0006 D-2/D-4).

To add a dialect: subclass :class:`ForgeDialect` (or :class:`SqlDialect` for
anything that speaks ``CREATE TABLE`` / ``INSERT``) in a sibling module,
declare ``name`` plus one physical type per :data:`~r2g.forge.core.JSON_TYPES`
entry, override only the hooks the target needs, and register the instance in
:data:`r2g.forge.dialects.DIALECTS`. The unit tests in ``tests/test_forge.py``
parametrize over the registry, so a new dialect is covered by the shared
contract tests (type table completeness, byte-identical rows) for free.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, List, Mapping

from ..core import JSON_TYPES, ForgeError, Rows, SchemaPlan, TablePlan


class ForgeDialect(ABC):
    """One target system's projection of the canonical plan and rows."""

    #: Registry key and the ``--dialect`` CLI value.
    name: ClassVar[str]
    #: File names :meth:`~r2g.forge.core.ForgeArtifacts.write_to` uses for
    #: ``ddl`` and ``load_sql``. SQL dialects keep the skeleton's names.
    ddl_filename: ClassVar[str] = "forge.sql"
    loader_filename: ClassVar[str] = "forge.load.sql"
    #: One physical type per conceptual JSON type. Must cover every
    #: :data:`~r2g.forge.core.JSON_TYPES` entry (asserted at import by
    #: :func:`check_type_table`).
    type_for_json: ClassVar[Mapping[str, str]]

    def physical_table(self, table: str) -> str:
        """Project a canonical snake_case table name onto the target's spelling.
        Default: unchanged."""
        return table

    def physical_column(self, column: str) -> str:
        """Project a canonical snake_case column name onto the target's spelling.
        Default: unchanged."""
        return column

    @abstractmethod
    def render_ddl(self, plan: SchemaPlan) -> str:
        """The schema definition (SQL DDL, or a manifest for non-SQL targets)."""

    @abstractmethod
    def render_loader(self, plan: SchemaPlan, rows: Rows, seed: int) -> str:
        """The loader that puts ``rows`` into the schema (INSERTs, or a script)."""


def check_type_table(dialect: ForgeDialect) -> None:
    """Refuse a dialect whose type table does not cover every JSON type."""
    missing = [t for t in JSON_TYPES if t not in dialect.type_for_json]
    if missing:
        raise ForgeError(f"dialect {dialect.name!r} declares no physical type for {missing}")


# ── Shared SQL machinery ────────────────────────────────────────────────


def sql_literal(value: Any, *, true: str = "TRUE", false: str = "FALSE") -> str:
    """Render one synthesized value as a SQL literal. Strings are
    single-quoted with ``''`` escaping, which every target here accepts."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return true if value else false
    if isinstance(value, (int, float)):
        return repr(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def split_sql_statements(sql: str) -> List[str]:
    """Split a loader/DDL script into individual statements.

    Drops ``--`` comment lines and splits on ``;`` outside single-quoted
    strings, so drivers that execute one statement per call (ClickHouse HTTP,
    Snowflake ``cursor.execute``) can replay a script verbatim.
    """
    body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    statements: List[str] = []
    current: List[str] = []
    in_string = False
    for ch in body:
        if ch == "'":
            in_string = not in_string
        if ch == ";" and not in_string:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            continue
        current.append(ch)
    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


class SqlDialect(ForgeDialect):
    """``CREATE TABLE`` + ``INSERT`` targets. Subclasses tune the hooks; the
    rendering skeleton is shared so every SQL dialect reads the same way."""

    #: Boolean literal spellings.
    true_literal: ClassVar[str] = "TRUE"
    false_literal: ClassVar[str] = "FALSE"
    #: One multi-row INSERT per table (warehouse-friendly) or one INSERT per
    #: row (the Postgres skeleton's shape).
    multi_row_insert: ClassVar[bool] = True

    # -- hooks ----------------------------------------------------------

    def ddl_header(self, plan: SchemaPlan) -> List[str]:
        """Extra comment lines after the standard DDL header."""
        return []

    def column_ddl(self, table: TablePlan, column_index: int) -> str:
        """One column definition line (without leading indent or comma)."""
        col = table.columns[column_index]
        physical_type = self.type_for_json[col.json_type]
        null = "" if col.nullable else " NOT NULL"
        return f"{self.physical_column(col.name)} {physical_type}{null}"

    def table_constraints(self, table: TablePlan) -> List[str]:
        """Constraint lines appended inside the ``CREATE TABLE`` body.
        Default: declared PK + FKs (F-4)."""
        lines = [f"PRIMARY KEY ({self.physical_column(table.primary_key.name)})"]
        for fk in table.foreign_keys:
            assert fk.references is not None
            lines.append(
                f"FOREIGN KEY ({self.physical_column(fk.name)}) REFERENCES "
                f"{self.physical_table(fk.references)} ({self.physical_column(table.primary_key.name)})"
            )
        return lines

    def table_suffix(self, table: TablePlan) -> str:
        """Text between the closing ``)`` and the ``;`` (e.g. an ENGINE clause)."""
        return ""

    def literal(self, value: Any) -> str:
        return sql_literal(value, true=self.true_literal, false=self.false_literal)

    # -- rendering ------------------------------------------------------

    def render_ddl(self, plan: SchemaPlan) -> str:
        parts: List[str] = [
            "-- Federation Forge — generated schema",
            f"-- dialect: {self.name}",
            "-- Regenerate with: r2g forge generate (see PLAN-federation-forge.md)",
            *self.ddl_header(plan),
            "",
        ]
        for table in plan.tables:
            lines = [self.column_ddl(table, i) for i in range(len(table.columns))]
            lines.extend(self.table_constraints(table))
            body = ",\n".join(f"    {line}" for line in lines)
            parts.append(
                f"CREATE TABLE {self.physical_table(table.table)} (\n{body}\n){self.table_suffix(table)};\n"
            )
        return "\n".join(parts)

    def render_loader(self, plan: SchemaPlan, rows: Rows, seed: int) -> str:
        parts: List[str] = [
            "-- Federation Forge — generated data",
            f"-- dialect: {self.name}; seed: {seed}",
            "",
        ]
        for table in plan.tables:
            physical = self.physical_table(table.table)
            columns = ", ".join(self.physical_column(c.name) for c in table.columns)
            tuples = [
                "(" + ", ".join(self.literal(row[c.name]) for c in table.columns) + ")"
                for row in rows[table.table]
            ]
            if self.multi_row_insert:
                values = ",\n".join(f"    {t}" for t in tuples)
                parts.append(f"INSERT INTO {physical} ({columns}) VALUES\n{values};")
            else:
                parts.extend(f"INSERT INTO {physical} ({columns}) VALUES {t};" for t in tuples)
            parts.append("")
        return "\n".join(parts)


__all__ = [
    "ForgeDialect",
    "SqlDialect",
    "check_type_table",
    "split_sql_statements",
    "sql_literal",
]
