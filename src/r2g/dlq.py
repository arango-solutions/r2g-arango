"""Dead-letter queue for failed ingestion records."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from r2g.log import get_logger

logger = get_logger(__name__)


class DeadLetterQueue:
    """Writes failed records to a JSONL file for later inspection."""

    def __init__(self, load_id: str, dlq_dir: str | Path | None = None) -> None:
        if dlq_dir is None:
            self._dir = Path.home() / ".r2g" / "dlq"
        else:
            self._dir = Path(dlq_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / f"{load_id}.jsonl"
        self._count = 0

    def record_failure(
        self,
        collection: str,
        row: dict[str, Any],
        error: str,
        source_table: str | None = None,
    ) -> None:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "collection": collection,
            "source_table": source_table or collection,
            "error": error,
            "row": row,
        }
        with self._path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
        self._count += 1

    @property
    def count(self) -> int:
        return self._count

    def read_errors(self, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        if not self._path.exists():
            return []
        entries: list[dict[str, Any]] = []
        with self._path.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i < offset:
                    continue
                if len(entries) >= limit:
                    break
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return entries

    @classmethod
    def list_dlq_files(cls, dlq_dir: str | Path | None = None) -> list[str]:
        d = Path(dlq_dir) if dlq_dir else Path.home() / ".r2g" / "dlq"
        if not d.exists():
            return []
        return [p.stem for p in d.glob("*.jsonl")]
