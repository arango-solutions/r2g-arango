"""``clickhouse`` — ClickHouse SQL, ``MergeTree`` tables ordered by the spine.

ClickHouse has neither enforced nor declared foreign keys, so the FK *intent*
is recorded where a reader can still find it: as a ``COMMENT`` on the FK
column and as header comments in the loader. This is the constraint-stripped
shape ADR-0006 D-3 names — the roundtrip's contract is two-branch: the
analyzers either recover the keys by inference or report them absent; a wrong
key or silence is the failure.

``ORDER BY (id)`` makes the surrogate key the table's sorting/primary key,
which ``system.columns.is_in_primary_key`` surfaces and r2g's
``ClickHouseConnector`` reads back as the PK.
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

from ..core import Rows, SchemaPlan, TablePlan
from .base import SqlDialect

CLICKHOUSE_TYPE_FOR_JSON_TYPE: Dict[str, str] = {
    "integer": "Int64",
    "float": "Float64",
    "boolean": "Bool",
    "string": "String",
}


def fk_intent_comment(references: str, key: str) -> str:
    """The machine-readable FK intent stored as a column comment."""
    return f"forge:foreign-key -> {references}({key})"


class ClickHouseDialect(SqlDialect):
    name: ClassVar[str] = "clickhouse"
    type_for_json: ClassVar[Dict[str, str]] = CLICKHOUSE_TYPE_FOR_JSON_TYPE
    true_literal: ClassVar[str] = "true"
    false_literal: ClassVar[str] = "false"

    def ddl_header(self, plan: SchemaPlan) -> List[str]:
        lines = [
            "-- ClickHouse declares no foreign keys; the FK intent below is recorded as",
            "-- column comments (constraint-stripped shape, ADR-0006 D-3 two-branch contract).",
        ]
        lines.extend(
            f"-- FOREIGN KEY intent: {e.from_table}.{e.fk_column} -> {e.to_table}(id)  [{e.relationship}]"
            for e in plan.edges
        )
        return lines

    def column_ddl(self, table: TablePlan, column_index: int) -> str:
        col = table.columns[column_index]
        physical_type = self.type_for_json[col.json_type]
        if col.nullable:
            physical_type = f"Nullable({physical_type})"
        line = f"{col.name} {physical_type}"
        if col.references is not None:
            comment = fk_intent_comment(col.references, table.primary_key.name)
            line += f" COMMENT '{comment}'"
        return line

    def table_constraints(self, table: TablePlan) -> List[str]:
        return []  # no constraint syntax in ClickHouse; see table_suffix

    def table_suffix(self, table: TablePlan) -> str:
        return f" ENGINE = MergeTree ORDER BY ({table.primary_key.name})"

    def render_loader(self, plan: SchemaPlan, rows: Rows, seed: int) -> str:
        header = [
            f"-- FOREIGN KEY intent (not enforceable in ClickHouse): "
            f"{e.from_table}.{e.fk_column} -> {e.to_table}(id)"
            for e in plan.edges
        ]
        body = super().render_loader(plan, rows, seed)
        if not header:
            return body
        lines = body.split("\n")
        # After the two standard header lines ("-- Federation Forge…", "-- dialect…").
        return "\n".join(lines[:2] + header + lines[2:])


__all__ = ["CLICKHOUSE_TYPE_FOR_JSON_TYPE", "ClickHouseDialect", "fk_intent_comment"]
