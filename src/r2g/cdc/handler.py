"""CDC event handler: consumes change events and applies deltas to ArangoDB.

The handler groups events by transaction when transaction_id is present,
and applies deltas in LSN order for cross-transaction consistency.
It is designed to be fed events from any source (logical replication
consumer, Kafka consumer, test harness, etc.).
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, Iterable

from r2g.cdc.conflict import ConflictPolicy, ConflictResolver
from r2g.cdc.delta_transformer import DeltaTransformer
from r2g.cdc.models import (
    ArangoDelta,
    ArangoOperation,
    ChangeEvent,
    TransactionBatch,
)
from r2g.connectors.arango_writer import ArangoWriter
from r2g.log import get_logger
from r2g.types import MappingConfig, Schema

logger = get_logger(__name__)

ProgressFn = Callable[[str, int, int], None]


class CDCStats:
    """Tracks CDC processing statistics."""

    def __init__(self) -> None:
        self.events_received: int = 0
        self.deltas_applied: int = 0
        self.deltas_failed: int = 0
        self.deltas_skipped: int = 0
        self.transactions_completed: int = 0
        self.last_lsn: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "events_received": self.events_received,
            "deltas_applied": self.deltas_applied,
            "deltas_failed": self.deltas_failed,
            "deltas_skipped": self.deltas_skipped,
            "transactions_completed": self.transactions_completed,
            "last_lsn": self.last_lsn,
        }


class CDCHandler:
    """Stateful handler that transforms and applies CDC events.

    Usage:
        handler = CDCHandler(writer, schema, config)
        handler.handle_event(event)        # one at a time
        handler.handle_events(events)      # batch
        handler.handle_transaction(events)  # grouped by txn

    Conflict resolution policy can be set via ``conflict_policy``:
    - ``source_wins`` (default) -- PG is truth; upsert on duplicate, insert on missing
    - ``last_write_wins`` -- compare LSN; reject stale writes
    - ``log_and_skip`` -- log conflicts, skip writes
    - ``fail`` -- raise on any conflict
    """

    def __init__(
        self,
        writer: ArangoWriter,
        schema: Schema,
        config: MappingConfig,
        on_progress: ProgressFn | None = None,
        conflict_policy: ConflictPolicy = ConflictPolicy.SOURCE_WINS,
    ) -> None:
        self.writer = writer
        self.transformer = DeltaTransformer(schema, config)
        self._on_progress = on_progress
        self.stats = CDCStats()
        self.resolver = ConflictResolver(policy=conflict_policy)

    def _apply_delta(self, delta: ArangoDelta, lsn: str | None = None) -> bool:
        """Apply a single delta with conflict resolution. Returns True on success."""
        try:
            self.writer.ensure_collection(delta.collection, edge=delta.is_edge)

            if delta.operation == ArangoOperation.INSERT:
                ok = self.resolver.resolve_insert(
                    self.writer, delta.collection, delta.document, lsn=lsn,
                )
            elif delta.operation == ArangoOperation.REPLACE:
                ok = self.resolver.resolve_replace(
                    self.writer, delta.collection, delta.document, lsn=lsn,
                )
            elif delta.operation == ArangoOperation.DELETE:
                ok = self.resolver.resolve_delete(
                    self.writer, delta.collection, delta.effective_key,
                )
            else:
                self.writer.apply_delta(delta)
                ok = True

            if ok:
                self.stats.deltas_applied += 1
            else:
                self.stats.deltas_skipped += 1
            return ok
        except Exception as exc:
            self.stats.deltas_failed += 1
            logger.warning(
                "cdc_delta_failed",
                collection=delta.collection,
                operation=delta.operation.value,
                key=delta.effective_key,
                error=str(exc),
            )
            if self.resolver.policy == ConflictPolicy.FAIL:
                raise
            return False

    def handle_event(self, event: ChangeEvent) -> list[ArangoDelta]:
        """Process a single change event end-to-end.

        Returns the list of deltas that were produced (whether or not
        they were applied successfully).
        """
        self.stats.events_received += 1
        deltas = self.transformer.transform(event)

        for delta in deltas:
            self._apply_delta(delta, lsn=event.lsn)

        if event.lsn:
            self.stats.last_lsn = event.lsn

        if self._on_progress:
            self._on_progress(
                "event",
                self.stats.events_received,
                self.stats.deltas_applied,
            )

        return deltas

    def handle_events(self, events: Iterable[ChangeEvent]) -> CDCStats:
        """Process multiple change events sequentially.

        Returns the accumulated stats after all events are processed.
        """
        for event in events:
            self.handle_event(event)
        return self.stats

    def handle_transaction(
        self, events: list[ChangeEvent]
    ) -> TransactionBatch:
        """Process a batch of events belonging to a single transaction.

        All deltas are collected first, then applied in order.
        This provides better atomicity semantics than handle_events.
        """
        batch = TransactionBatch(
            transaction_id=events[0].transaction_id if events else None,
            lsn=events[-1].lsn if events else None,
            source_events=len(events),
        )

        all_deltas: list[tuple[ArangoDelta, str | None]] = []
        for event in events:
            self.stats.events_received += 1
            deltas = self.transformer.transform(event)
            for d in deltas:
                all_deltas.append((d, event.lsn))

        for delta, lsn in all_deltas:
            self._apply_delta(delta, lsn=lsn)
            batch.deltas.append(delta)

        self.stats.transactions_completed += 1
        if batch.lsn:
            self.stats.last_lsn = batch.lsn

        return batch

    def group_by_transaction(
        self, events: Iterable[ChangeEvent]
    ) -> list[list[ChangeEvent]]:
        """Group events by transaction_id for batched application.

        Events without a transaction_id are placed in their own
        single-event groups.  Order within each group is preserved.
        """
        groups: dict[int | None, list[ChangeEvent]] = defaultdict(list)
        order: list[int | None] = []

        for event in events:
            tid = event.transaction_id
            if tid not in groups:
                order.append(tid)
            groups[tid].append(event)

        result = []
        for tid in order:
            if tid is None:
                for ev in groups[tid]:
                    result.append([ev])
            else:
                result.append(groups[tid])
        return result
