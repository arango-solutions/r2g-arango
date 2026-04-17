from __future__ import annotations

import json
from typing import Any, Dict, Optional

from r2g.config import pg_type_to_json_type
from r2g.expressions import CompiledExpression, ExpressionError, compile_expression
from r2g.log import get_logger
from r2g.types import CollectionMapping, Column, FieldExpression, Table

logger = get_logger(__name__)


class NodeTransformer:
    def __init__(
        self,
        table_def: Table,
        collection_mapping: Optional[CollectionMapping] = None,
        key_separator: str = "_",
        type_overrides: Optional[Dict[str, str]] = None,
    ) -> None:
        self.table_def = table_def
        self._mapping = collection_mapping
        self.key_separator = key_separator
        self._type_overrides = type_overrides or {}
        self._compiled_expressions: list[tuple[FieldExpression, Optional[CompiledExpression]]] = []
        if collection_mapping is not None:
            for fx in collection_mapping.field_expressions:
                if fx.is_identity or not fx.expression.strip():
                    self._compiled_expressions.append((fx, None))
                    continue
                if fx.engine != "aql":
                    logger.warning(
                        "field_expression_engine_unsupported",
                        target=fx.target,
                        engine=fx.engine,
                    )
                    self._compiled_expressions.append((fx, None))
                    continue
                try:
                    compiled = compile_expression(fx.expression)
                except ExpressionError as err:
                    logger.warning(
                        "field_expression_compile_failed",
                        target=fx.target,
                        engine=fx.engine,
                        error=str(err),
                    )
                    self._compiled_expressions.append((fx, None))
                    continue
                self._compiled_expressions.append((fx, compiled))

    def _json_type_for_column(self, column: Column) -> str:
        if column.name in self._type_overrides:
            return self._type_overrides[column.name]
        return pg_type_to_json_type(column.data_type)

    def _coerce_value(self, value: Any, column: Column) -> Any:
        json_type = self._json_type_for_column(column)
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "" and column.is_nullable:
            return None

        if json_type == "integer":
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            return int(str(value).strip())

        if json_type == "float":
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
            return float(str(value).strip())

        if json_type == "boolean":
            if isinstance(value, str):
                return value.lower() in ("true", "1", "t", "yes")
            return bool(value)

        if json_type == "object":
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    logger.warning("json_decode_failed", column=column.name)
                    return value
            return value

        if json_type == "array":
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    pass
                pg = value.strip()
                if pg.startswith("{") and pg.endswith("}"):
                    inner = pg[1:-1]
                    if inner == "":
                        return []
                    return [elem.strip('"') for elem in inner.split(",")]
                return value
            return value

        return str(value)

    def _generate_key(self, row: Dict[str, Any]) -> Optional[str]:
        if not self.table_def.primary_key:
            return None
        pk_values: list[str] = []
        for pk_col in self.table_def.primary_key:
            val = row.get(pk_col)
            if val is None:
                raise ValueError(f"Row missing PK value for column '{pk_col}': {row}")
            pk_values.append(str(val))
        return self.key_separator.join(pk_values)

    def transform_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        key = self._generate_key(row)

        if self._mapping is None:
            doc = row.copy()
            if key:
                doc["_key"] = key
            return doc

        col_by_name: Dict[str, Column] = {c.name: c for c in self.table_def.columns}
        known = set(col_by_name.keys())
        names = set(row.keys()) & known
        if self._mapping.include_fields is not None:
            names &= set(self._mapping.include_fields)
        names -= set(self._mapping.exclude_fields)

        expression_targets = {fx.target for fx, _ in self._compiled_expressions}

        ordered_names = [k for k in row if k in names]
        doc: Dict[str, Any] = {}
        for src_name in ordered_names:
            raw = row.get(src_name)
            column = col_by_name[src_name]
            try:
                coerced = self._coerce_value(raw, column)
            except (TypeError, ValueError) as e:
                logger.warning("coerce_failed", column=src_name, error=str(e))
                coerced = raw
            tgt_name = self._mapping.field_mappings.get(src_name, src_name)
            if tgt_name in expression_targets:
                continue
            doc[tgt_name] = coerced

        for fx, compiled in self._compiled_expressions:
            doc[fx.target] = self._apply_field_expression(fx, compiled, row, col_by_name)

        if key:
            doc["_key"] = key
        return doc

    def _apply_field_expression(
        self,
        fx: FieldExpression,
        compiled: Optional[CompiledExpression],
        row: Dict[str, Any],
        col_by_name: Dict[str, Column],
    ) -> Any:
        """Evaluate a ``FieldExpression`` against the raw source row.

        Identity and un-compilable expressions fall back to a pass-through
        read of ``sources`` (or the target name if ``sources`` is empty).
        """

        if compiled is None:
            src_name = fx.sources[0] if fx.sources else fx.target
            raw = row.get(src_name)
            column = col_by_name.get(src_name)
            if column is None:
                return raw
            try:
                return self._coerce_value(raw, column)
            except (TypeError, ValueError) as e:
                logger.warning("coerce_failed", column=src_name, error=str(e))
                return raw

        env: Dict[str, Any] = {}
        source_names = fx.sources or list(compiled.references)
        for src_name in source_names:
            raw = row.get(src_name)
            column = col_by_name.get(src_name)
            if column is None:
                env[src_name] = raw
                continue
            try:
                env[src_name] = self._coerce_value(raw, column)
            except (TypeError, ValueError) as e:
                logger.warning("coerce_failed", column=src_name, error=str(e))
                env[src_name] = raw
        for ref in compiled.references:
            env.setdefault(ref, row.get(ref))
        try:
            return compiled.evaluate(env)
        except ExpressionError as err:
            logger.warning(
                "field_expression_eval_failed",
                target=fx.target,
                error=str(err),
            )
            return None
