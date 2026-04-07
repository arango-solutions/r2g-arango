"""Validate referential integrity of dump data against a schema and mapping.

Reads CSV dump files, builds PK lookup sets per table, then checks every
FK column value to ensure the referenced PK exists. Reports orphaned
references that would produce broken edges in ArangoDB.
"""
from __future__ import annotations

from pathlib import Path

from r2g.input.dump_reader import DumpReader
from r2g.log import get_logger
from r2g.types import MappingConfig, Schema

logger = get_logger(__name__)


class ValidationIssue:
    """A single referential integrity violation."""

    __slots__ = ("source_table", "fk_column", "target_table", "orphan_value", "row_number")

    def __init__(
        self,
        source_table: str,
        fk_column: str,
        target_table: str,
        orphan_value: str,
        row_number: int,
    ) -> None:
        self.source_table = source_table
        self.fk_column = fk_column
        self.target_table = target_table
        self.orphan_value = orphan_value
        self.row_number = row_number

    def __repr__(self) -> str:
        return (
            f"{self.source_table}.{self.fk_column} row {self.row_number}: "
            f"value '{self.orphan_value}' not found in {self.target_table} PKs"
        )


class ValidationReport:
    """Aggregated results of data validation."""

    def __init__(self) -> None:
        self.issues: list[ValidationIssue] = []
        self.tables_checked: int = 0
        self.fk_checks: int = 0
        self.rows_scanned: int = 0
        self.pk_sets_built: int = 0

    @property
    def is_clean(self) -> bool:
        return len(self.issues) == 0

    def summary_by_fk(self) -> dict[str, int]:
        """Count of orphans grouped by 'table.column -> target'."""
        counts: dict[str, int] = {}
        for issue in self.issues:
            key = f"{issue.source_table}.{issue.fk_column} -> {issue.target_table}"
            counts[key] = counts.get(key, 0) + 1
        return counts


def _build_pk_set(
    dump_path: Path,
    pk_columns: list[str],
    key_separator: str,
) -> set[str]:
    """Read a dump file and build a set of composite PK values."""
    reader = DumpReader(str(dump_path))
    pk_set: set[str] = set()
    for row in reader.read_rows():
        parts = [str(row.get(col, "")) for col in pk_columns]
        pk_set.add(key_separator.join(parts))
    return pk_set


def validate_data(
    schema: Schema,
    config: MappingConfig,
    dumps_dir: str | Path,
    file_pattern: str = "*.csv",
    max_issues_per_fk: int = 100,
) -> ValidationReport:
    """Check referential integrity of dump data.

    For each FK in the schema, checks that every FK value in the source
    table's dump file corresponds to an existing PK value in the target
    table's dump file. Returns a report of orphaned references.
    """
    report = ValidationReport()
    dump_dir = Path(dumps_dir)

    dump_by_table: dict[str, Path] = {}
    for fpath in sorted(dump_dir.glob(file_pattern)):
        if fpath.is_file():
            dump_by_table[fpath.stem] = fpath

    pk_cache: dict[str, set[str]] = {}
    key_sep = config.key_separator

    for table_name, table in schema.tables.items():
        if table_name not in dump_by_table:
            continue
        if not table.primary_key:
            continue
        if table_name not in pk_cache:
            pk_cache[table_name] = _build_pk_set(
                dump_by_table[table_name], table.primary_key, key_sep
            )
            report.pk_sets_built += 1

    for table_name, table in schema.tables.items():
        if not table.foreign_keys:
            continue
        if table_name not in dump_by_table:
            continue

        report.tables_checked += 1
        reader = DumpReader(str(dump_by_table[table_name]))

        issue_counts: dict[str, int] = {}

        for row_num, row in enumerate(reader.read_rows(), start=1):
            report.rows_scanned += 1
            for fk in table.foreign_keys:
                target = fk.foreign_table
                if target not in pk_cache:
                    if target in dump_by_table and target in schema.tables:
                        target_table = schema.tables[target]
                        if target_table.primary_key:
                            pk_cache[target] = _build_pk_set(
                                dump_by_table[target], target_table.primary_key, key_sep
                            )
                            report.pk_sets_built += 1
                    else:
                        continue

                target_pks = pk_cache.get(target)
                if target_pks is None:
                    continue

                report.fk_checks += 1

                fk_parts = [str(row.get(col, "")) for col in fk.columns]
                if all(v == "" or v == "None" or v is None for v in fk_parts):
                    continue

                fk_value = key_sep.join(fk_parts)
                if fk_value not in target_pks:
                    fk_key = f"{table_name}.{','.join(fk.columns)}"
                    current = issue_counts.get(fk_key, 0)
                    if current < max_issues_per_fk:
                        report.issues.append(
                            ValidationIssue(
                                source_table=table_name,
                                fk_column=",".join(fk.columns),
                                target_table=target,
                                orphan_value=fk_value,
                                row_number=row_num,
                            )
                        )
                    issue_counts[fk_key] = current + 1

    return report
