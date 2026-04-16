from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from r2g.connectors.arango_writer import ArangoWriter
from r2g.log import get_logger
from r2g.mapping_diff import ReloadAction, ReloadPlan

logger = get_logger(__name__)


@dataclass
class ReloadReport:
    actions_executed: list[dict[str, str]] = field(default_factory=list)
    actions_skipped: list[dict[str, str]] = field(default_factory=list)
    errors: list[dict[str, str]] = field(default_factory=list)
    rows_reloaded: int = 0


class SelectiveReloader:
    def __init__(
        self,
        writer: ArangoWriter,
        plan: ReloadPlan,
        pg_conn_string: str | None = None,
        schema: Any = None,
        config: Any = None,
        batch_size: int = 10000,
        on_duplicate: str = "replace",
        pg_schema: str = "public",
    ) -> None:
        self.writer = writer
        self.plan = plan
        self.pg_conn_string = pg_conn_string
        self.schema = schema
        self.config = config
        self.batch_size = batch_size
        self.on_duplicate = on_duplicate
        self.pg_schema = pg_schema

    def execute(self, dry_run: bool = False) -> ReloadReport:
        """Execute the reload plan. Returns a report of what was done."""
        report = ReloadReport()
        for action in self.plan.actions:
            try:
                if dry_run:
                    report.actions_skipped.append({
                        "action": action.action_type,
                        "collection": action.collection,
                        "reason": action.reason,
                    })
                    continue
                self._dispatch(action, report)
            except Exception as e:
                report.errors.append({
                    "action": action.action_type,
                    "collection": action.collection,
                    "error": str(e),
                })
                logger.error(
                    "selective_reload_action_failed",
                    action=action.action_type,
                    collection=action.collection,
                    error=str(e),
                )
        return report

    def _dispatch(self, action: ReloadAction, report: ReloadReport) -> None:
        handlers = {
            "rename_collection": self._rename_collection,
            "drop_collection": self._drop_collection,
            "reload_collection": self._reload_collection,
            "drop_edge": self._drop_edge,
            "reload_edge": self._reload_edge,
            "aql_update": self._aql_update,
        }
        handler = handlers.get(action.action_type)
        if handler is None:
            logger.warning("unknown_action_type", action_type=action.action_type)
            return
        handler(action, report)

    def _rename_collection(self, action: ReloadAction, report: ReloadReport) -> None:
        old_name = action.collection
        new_name = action.reason.split("'")[1] if "'" in action.reason else action.collection
        try:
            self.writer.db.collection(old_name).rename(new_name)
        except Exception:
            logger.warning("rename_collection_fallback", old=old_name, new=new_name)
        report.actions_executed.append({
            "action": "rename_collection",
            "collection": f"{old_name} -> {new_name}",
            "reason": action.reason,
        })

    def _drop_collection(self, action: ReloadAction, report: ReloadReport) -> None:
        self.writer.drop_collection(action.collection)
        report.actions_executed.append({
            "action": "drop_collection",
            "collection": action.collection,
            "reason": action.reason,
        })

    def _drop_edge(self, action: ReloadAction, report: ReloadReport) -> None:
        self.writer.drop_collection(action.collection)
        report.actions_executed.append({
            "action": "drop_edge",
            "collection": action.collection,
            "reason": action.reason,
        })

    def _reload_collection(self, action: ReloadAction, report: ReloadReport) -> None:
        """Reload a single document collection from PostgreSQL."""
        if self.pg_conn_string is None or self.schema is None or self.config is None:
            report.actions_skipped.append({
                "action": "reload_collection",
                "collection": action.collection,
                "reason": "no PostgreSQL connection configured",
            })
            return

        from r2g.streaming.pipeline import StreamingPipeline

        target_tables = set()
        for cm in self.config.collections.values():
            if cm.target_collection == action.collection:
                target_tables.add(cm.source_table)

        if not target_tables:
            report.actions_skipped.append({
                "action": "reload_collection",
                "collection": action.collection,
                "reason": "no source table found for this collection",
            })
            return

        pipeline = StreamingPipeline(
            pg_conn_string=self.pg_conn_string,
            arango_writer=self.writer,
            schema=self.schema,
            config=self.config,
            batch_size=self.batch_size,
            on_duplicate=self.on_duplicate,
            pg_schema=self.pg_schema,
            drop_collections=True,
            include_tables=target_tables,
        )
        results = pipeline.run()
        rows = sum(c for _, c in results.get("documents", []))
        report.rows_reloaded += rows
        report.actions_executed.append({
            "action": "reload_collection",
            "collection": action.collection,
            "reason": action.reason,
            "rows": str(rows),
        })

    def _reload_edge(self, action: ReloadAction, report: ReloadReport) -> None:
        """Reload a single edge collection from PostgreSQL."""
        if self.pg_conn_string is None or self.schema is None or self.config is None:
            report.actions_skipped.append({
                "action": "reload_edge",
                "collection": action.collection,
                "reason": "no PostgreSQL connection configured",
            })
            return

        from r2g.streaming.pipeline import StreamingPipeline

        edge_def = None
        for e in self.config.edges:
            if e.edge_collection == action.collection:
                edge_def = e
                break

        if edge_def is None:
            report.actions_skipped.append({
                "action": "reload_edge",
                "collection": action.collection,
                "reason": "edge definition not found in config",
            })
            return

        self.writer.drop_collection(action.collection)
        self.writer.ensure_collection(action.collection, edge=True)

        pipeline = StreamingPipeline(
            pg_conn_string=self.pg_conn_string,
            arango_writer=self.writer,
            schema=self.schema,
            config=self.config,
            batch_size=self.batch_size,
            on_duplicate=self.on_duplicate,
            pg_schema=self.pg_schema,
            include_tables={edge_def.from_collection},
        )
        results = pipeline.run()
        rows = sum(c for _, c in results.get("edges", []))
        report.rows_reloaded += rows
        report.actions_executed.append({
            "action": "reload_edge",
            "collection": action.collection,
            "reason": action.reason,
            "rows": str(rows),
        })

    def _aql_update(self, action: ReloadAction, report: ReloadReport) -> None:
        """Execute an AQL query for in-place data migration."""
        if action.aql_query is None:
            report.actions_skipped.append({
                "action": "aql_update",
                "collection": action.collection,
                "reason": "no AQL query provided",
            })
            return

        try:
            self.writer.db.aql.execute(
                action.aql_query,
                bind_vars={"@edge_collection": action.collection, "@coll": action.collection},
            )
        except Exception as e:
            try:
                self.writer.db.aql.execute(
                    action.aql_query,
                    bind_vars={"@coll": action.collection},
                )
            except Exception:
                raise e
        report.actions_executed.append({
            "action": "aql_update",
            "collection": action.collection,
            "reason": action.reason,
        })
