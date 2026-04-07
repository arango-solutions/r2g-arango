"""CDC event models for PostgreSQL logical replication changes.

A ChangeEvent represents a single row-level mutation captured via
logical decoding (pgoutput).  The event carries enough context to
reconstruct the ArangoDB delta without re-querying PostgreSQL.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class ChangeOperation(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class ChangeEvent(BaseModel):
    """A single row-level change from PostgreSQL logical replication.

    Attributes:
        operation: The DML operation type.
        schema_name: PostgreSQL schema (e.g. "public").
        table_name: Source table that emitted the change.
        new_row: Column values after the change (INSERT/UPDATE).
        old_row: Column values before the change (UPDATE/DELETE).
                 Requires REPLICA IDENTITY FULL for non-PK columns.
        lsn: PostgreSQL Log Sequence Number for ordering.
        timestamp: Wall-clock time of the transaction commit.
        transaction_id: PostgreSQL transaction ID (xid) for grouping.
    """

    operation: ChangeOperation
    schema_name: str = "public"
    table_name: str
    new_row: Optional[Dict[str, Any]] = None
    old_row: Optional[Dict[str, Any]] = None
    lsn: Optional[str] = None
    timestamp: Optional[datetime] = None
    transaction_id: Optional[int] = None

    @property
    def is_insert(self) -> bool:
        return self.operation == ChangeOperation.INSERT

    @property
    def is_update(self) -> bool:
        return self.operation == ChangeOperation.UPDATE

    @property
    def is_delete(self) -> bool:
        return self.operation == ChangeOperation.DELETE

    @property
    def effective_row(self) -> Dict[str, Any]:
        """Return the row data to use for transformation.

        For INSERT/UPDATE this is new_row; for DELETE this is old_row.
        """
        if self.operation == ChangeOperation.DELETE:
            return self.old_row or {}
        return self.new_row or {}


class ArangoOperation(str, Enum):
    """Target operation to apply against ArangoDB."""

    INSERT = "INSERT"
    REPLACE = "REPLACE"
    DELETE = "DELETE"


class ArangoDelta(BaseModel):
    """A single mutation to apply to ArangoDB.

    Produced by the DeltaTransformer from a ChangeEvent.
    """

    operation: ArangoOperation
    collection: str
    is_edge: bool = False
    document: Dict[str, Any] = Field(default_factory=dict)
    key: Optional[str] = None

    @property
    def effective_key(self) -> str:
        """Return the document _key, from explicit key or document body."""
        return self.key or self.document.get("_key", "")


class TransactionBatch(BaseModel):
    """A group of ArangoDeltas from a single PostgreSQL transaction.

    Deltas within a batch should be applied atomically (or at minimum
    in order) to maintain consistency.
    """

    transaction_id: Optional[int] = None
    lsn: Optional[str] = None
    deltas: list[ArangoDelta] = Field(default_factory=list)
    source_events: int = 0
