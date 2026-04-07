"""Conflict resolution for CDC delta application.

When applying CDC deltas to ArangoDB, several conflict scenarios arise:

- INSERT on an existing _key (duplicate/replay)
- REPLACE on a missing _key (late delete crossed with update)
- Edge referencing a deleted vertex (orphan edge)
- Out-of-order events (stale overwrite)

This module provides configurable policies to handle these scenarios.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel

from r2g.log import get_logger

logger = get_logger(__name__)


class ConflictPolicy(str, Enum):
    """How to handle write conflicts during CDC application."""

    SOURCE_WINS = "source_wins"
    """PostgreSQL is the source of truth.  INSERT becomes upsert
    (overwrite=True); REPLACE falls back to insert if missing;
    DELETE is idempotent.  This is the safest default."""

    LAST_WRITE_WINS = "last_write_wins"
    """Compare event LSN/timestamp against a stored watermark.
    Reject events whose LSN is older than the last successfully
    applied LSN for the same _key.  Requires per-document LSN
    tracking in ArangoDB (via a ``_r2g_lsn`` field)."""

    LOG_AND_SKIP = "log_and_skip"
    """Log every conflict as a warning and skip the failing
    write.  Useful for monitoring conflict frequency before
    choosing a stricter policy."""

    FAIL = "fail"
    """Raise an error on the first conflict.  Suitable for
    pipelines where conflicts indicate a bug that must be fixed."""


class ConflictType(str, Enum):
    INSERT_DUPLICATE = "insert_duplicate"
    REPLACE_MISSING = "replace_missing"
    DELETE_MISSING = "delete_missing"
    STALE_OVERWRITE = "stale_overwrite"
    ORPHAN_EDGE = "orphan_edge"


class ConflictEvent(BaseModel):
    """Record of a detected conflict."""

    conflict_type: ConflictType
    collection: str
    key: str = ""
    policy: ConflictPolicy
    resolved: bool = False
    resolution: str = ""
    details: str = ""


class ConflictLog:
    """Accumulates conflict events during a CDC session."""

    def __init__(self) -> None:
        self.events: list[ConflictEvent] = []
        self._counts: dict[ConflictType, int] = {}

    def record(self, event: ConflictEvent) -> None:
        self.events.append(event)
        self._counts[event.conflict_type] = (
            self._counts.get(event.conflict_type, 0) + 1
        )

    @property
    def total(self) -> int:
        return len(self.events)

    def counts(self) -> dict[str, int]:
        return {k.value: v for k, v in self._counts.items()}

    def summary(self) -> dict[str, Any]:
        return {
            "total_conflicts": self.total,
            "by_type": self.counts(),
        }


class ConflictResolver:
    """Applies conflict resolution policy to CDC write operations.

    The resolver wraps ArangoWriter operations and intercepts
    exceptions that indicate a conflict scenario, then resolves
    them according to the configured policy.
    """

    def __init__(
        self,
        policy: ConflictPolicy = ConflictPolicy.SOURCE_WINS,
    ) -> None:
        self.policy = policy
        self.log = ConflictLog()

    def resolve_insert(
        self,
        writer: Any,
        collection: str,
        document: dict[str, Any],
        lsn: str | None = None,
    ) -> bool:
        """Attempt an INSERT, resolving duplicates per policy."""
        try:
            writer.insert_document(collection, document)
            return True
        except Exception as exc:
            if not self._is_duplicate_error(exc):
                raise

            conflict = ConflictEvent(
                conflict_type=ConflictType.INSERT_DUPLICATE,
                collection=collection,
                key=document.get("_key", ""),
                policy=self.policy,
                details=str(exc),
            )

            if self.policy == ConflictPolicy.SOURCE_WINS:
                writer.replace_document(collection, document)
                conflict.resolved = True
                conflict.resolution = "upsert (replaced existing)"
                self.log.record(conflict)
                logger.info(
                    "conflict_resolved_upsert",
                    collection=collection,
                    key=document.get("_key"),
                )
                return True

            if self.policy == ConflictPolicy.LAST_WRITE_WINS:
                if self._should_overwrite(writer, collection, document, lsn):
                    writer.replace_document(collection, document)
                    conflict.resolved = True
                    conflict.resolution = "overwritten (newer LSN)"
                else:
                    conflict.resolved = True
                    conflict.resolution = "skipped (stale LSN)"
                self.log.record(conflict)
                return conflict.resolution.startswith("overwritten")

            if self.policy == ConflictPolicy.LOG_AND_SKIP:
                conflict.resolved = True
                conflict.resolution = "skipped"
                self.log.record(conflict)
                logger.warning(
                    "conflict_skipped",
                    type="insert_duplicate",
                    collection=collection,
                    key=document.get("_key"),
                )
                return False

            # FAIL policy
            self.log.record(conflict)
            raise

    def resolve_replace(
        self,
        writer: Any,
        collection: str,
        document: dict[str, Any],
        lsn: str | None = None,
    ) -> bool:
        """Attempt a REPLACE, resolving missing documents per policy."""
        try:
            if self.policy == ConflictPolicy.LAST_WRITE_WINS:
                document = self._stamp_lsn(document, lsn)
            writer.replace_document(collection, document)
            return True
        except Exception as exc:
            if not self._is_not_found_error(exc):
                raise

            conflict = ConflictEvent(
                conflict_type=ConflictType.REPLACE_MISSING,
                collection=collection,
                key=document.get("_key", ""),
                policy=self.policy,
                details=str(exc),
            )

            if self.policy == ConflictPolicy.SOURCE_WINS:
                writer.insert_document(collection, document)
                conflict.resolved = True
                conflict.resolution = "inserted (was missing)"
                self.log.record(conflict)
                return True

            if self.policy in (
                ConflictPolicy.LAST_WRITE_WINS,
                ConflictPolicy.LOG_AND_SKIP,
            ):
                conflict.resolved = True
                conflict.resolution = "skipped (target missing)"
                self.log.record(conflict)
                logger.warning(
                    "conflict_skipped",
                    type="replace_missing",
                    collection=collection,
                    key=document.get("_key"),
                )
                return False

            self.log.record(conflict)
            raise

    def resolve_delete(
        self,
        writer: Any,
        collection: str,
        key: str,
    ) -> bool:
        """Attempt a DELETE, resolving missing documents per policy.

        DELETE with ignore_missing=True is already idempotent in the
        writer, so conflicts here are rare.  This method exists for
        completeness and conflict logging.
        """
        writer.delete_document(collection, key)
        return True

    # ------------------------------------------------------------------
    # LWW helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _stamp_lsn(
        document: dict[str, Any], lsn: str | None
    ) -> dict[str, Any]:
        """Inject ``_r2g_lsn`` into the document for LWW tracking."""
        if lsn is not None:
            document = {**document, "_r2g_lsn": lsn}
        return document

    @staticmethod
    def _should_overwrite(
        writer: Any,
        collection: str,
        document: dict[str, Any],
        lsn: str | None,
    ) -> bool:
        """Check if the incoming LSN is newer than what's stored."""
        if lsn is None:
            return True
        key = document.get("_key")
        if key is None:
            return True
        try:
            coll = writer.db.collection(collection)
            existing = coll.get(key)
            if existing is None:
                return True
            stored_lsn = existing.get("_r2g_lsn")
            if stored_lsn is None:
                return True
            return lsn >= stored_lsn
        except Exception:
            return True

    # ------------------------------------------------------------------
    # Error classification
    # ------------------------------------------------------------------

    @staticmethod
    def _is_duplicate_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        return "unique constraint" in msg or "1210" in msg or "conflict" in msg

    @staticmethod
    def _is_not_found_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        return "not found" in msg or "1202" in msg or "document not found" in msg
