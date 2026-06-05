from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from r2g.connectors.arango_writer import ArangoWriter
from r2g.log import get_logger
from r2g.mapping_diff import ReloadAction, ReloadPlan
from r2g.types import RESERVED_ATTRIBUTES

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
        source_connector: Any = None,
        graph_name: str | None = None,
    ) -> None:
        self.writer = writer
        self.plan = plan
        self.pg_conn_string = pg_conn_string
        self.schema = schema
        self.config = config
        self.batch_size = batch_size
        self.on_duplicate = on_duplicate
        self.pg_schema = pg_schema
        self.source_connector = source_connector
        self.graph_name = graph_name

    def _has_source(self) -> bool:
        return self.source_connector is not None or self.pg_conn_string is not None

    def _make_pipeline(self, *, include_tables: set[str], drop_collections: bool):
        """Build a StreamingPipeline for a targeted reload, preferring a live
        source connector over a raw connection string."""
        from r2g.streaming.pipeline import StreamingPipeline

        kwargs: dict[str, Any] = dict(
            arango_writer=self.writer,
            schema=self.schema,
            config=self.config,
            batch_size=self.batch_size,
            on_duplicate=self.on_duplicate,
            pg_schema=self.pg_schema,
            drop_collections=drop_collections,
            include_tables=include_tables,
        )
        if self.source_connector is not None:
            kwargs["source_connector"] = self.source_connector
        else:
            kwargs["pg_conn_string"] = self.pg_conn_string
        return StreamingPipeline(**kwargs)

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
            "rebuild_graph": self._rebuild_graph,
        }
        handler = handlers.get(action.action_type)
        if handler is None:
            logger.warning("unknown_action_type", action_type=action.action_type)
            return
        handler(action, report)

    def _rename_collection(self, action: ReloadAction, report: ReloadReport) -> None:
        old_name = action.params.get("old_name") or action.collection
        new_name = action.params.get("new_name") or (
            action.reason.split("'")[1] if "'" in action.reason else action.collection
        )
        if old_name == new_name:
            return
        # Idempotent: if the new collection already exists (e.g. rerun), skip.
        if self.writer.db.has_collection(new_name):
            report.actions_skipped.append({
                "action": "rename_collection",
                "collection": f"{old_name} -> {new_name}",
                "reason": "target collection already exists",
            })
            return
        if not self.writer.db.has_collection(old_name):
            report.actions_skipped.append({
                "action": "rename_collection",
                "collection": f"{old_name} -> {new_name}",
                "reason": "source collection not found",
            })
            return
        self.writer.db.collection(old_name).rename(new_name)
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
        """Reload a single document collection from the source."""
        if not self._has_source() or self.schema is None or self.config is None:
            report.actions_skipped.append({
                "action": "reload_collection",
                "collection": action.collection,
                "reason": "no PostgreSQL connection configured",
            })
            return

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

        pipeline = self._make_pipeline(include_tables=target_tables, drop_collections=True)
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
        """Reload a single edge collection from the source."""
        if not self._has_source() or self.schema is None or self.config is None:
            report.actions_skipped.append({
                "action": "reload_edge",
                "collection": action.collection,
                "reason": "no PostgreSQL connection configured",
            })
            return

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

        pipeline = self._make_pipeline(
            include_tables={edge_def.from_collection}, drop_collections=False
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

        # Never let an attribute-rename touch an ArangoDB system attribute.
        if RESERVED_ATTRIBUTES & {action.params.get("old_name"), action.params.get("new_name")}:
            report.actions_skipped.append({
                "action": "aql_update",
                "collection": action.collection,
                "reason": "refusing to rename a reserved system attribute",
            })
            return

        # Collection-name binds plus any structured params the planner attached
        # (e.g. {"old_name", "new_name"} for an attribute rename).
        bind_vars: dict[str, Any] = {
            "@edge_collection": action.collection,
            "@coll": action.collection,
        }
        bind_vars.update(action.params)
        try:
            self.writer.db.aql.execute(action.aql_query, bind_vars=bind_vars)
        except Exception as e:
            # Retry without the (often unused) edge-collection bind for queries
            # that only reference @@coll.
            retry_vars = {k: v for k, v in bind_vars.items() if k != "@edge_collection"}
            try:
                self.writer.db.aql.execute(action.aql_query, bind_vars=retry_vars)
            except Exception:
                raise e
        report.actions_executed.append({
            "action": "aql_update",
            "collection": action.collection,
            "reason": action.reason,
        })

    def _rebuild_graph(self, action: ReloadAction, report: ReloadReport) -> None:
        """Recreate the named graph so its edge definitions reference current
        collection names (collections themselves are preserved)."""
        if not self.graph_name or self.config is None:
            report.actions_skipped.append({
                "action": "rebuild_graph",
                "collection": self.graph_name or "",
                "reason": "no graph name or config available",
            })
            return
        from r2g.config import ConfigManager

        edge_defs = ConfigManager.graph_edge_definitions(self.config)
        self.writer.create_named_graph(self.graph_name, edge_defs)
        report.actions_executed.append({
            "action": "rebuild_graph",
            "collection": self.graph_name,
            "reason": action.reason,
        })
