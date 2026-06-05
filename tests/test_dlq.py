from __future__ import annotations

import json

from r2g.dlq import DeadLetterQueue


class TestDeadLetterQueue:
    def test_record_failure_writes_jsonl(self, tmp_path):
        dlq = DeadLetterQueue("load-001", dlq_dir=tmp_path)
        dlq.record_failure(
            collection="users",
            row={"_key": "1", "name": "Alice"},
            error="duplicate key",
            source_table="public.users",
        )

        path = tmp_path / "load-001.jsonl"
        assert path.exists()
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 1

        entry = json.loads(lines[0])
        assert entry["collection"] == "users"
        assert entry["source_table"] == "public.users"
        assert entry["error"] == "duplicate key"
        assert entry["row"]["_key"] == "1"
        assert "timestamp" in entry

    def test_count_property(self, tmp_path):
        dlq = DeadLetterQueue("load-002", dlq_dir=tmp_path)
        assert dlq.count == 0

        dlq.record_failure("col_a", {"id": 1}, "err1")
        dlq.record_failure("col_b", {"id": 2}, "err2")
        dlq.record_failure("col_a", {"id": 3}, "err3")
        assert dlq.count == 3

    def test_read_errors_returns_all(self, tmp_path):
        dlq = DeadLetterQueue("load-003", dlq_dir=tmp_path)
        for i in range(5):
            dlq.record_failure("col", {"id": i}, f"error {i}")

        errors = dlq.read_errors()
        assert len(errors) == 5
        assert errors[0]["row"]["id"] == 0
        assert errors[4]["row"]["id"] == 4

    def test_read_errors_with_limit(self, tmp_path):
        dlq = DeadLetterQueue("load-004", dlq_dir=tmp_path)
        for i in range(10):
            dlq.record_failure("col", {"id": i}, f"error {i}")

        errors = dlq.read_errors(limit=3)
        assert len(errors) == 3
        assert errors[0]["row"]["id"] == 0

    def test_read_errors_with_offset(self, tmp_path):
        dlq = DeadLetterQueue("load-005", dlq_dir=tmp_path)
        for i in range(10):
            dlq.record_failure("col", {"id": i}, f"error {i}")

        errors = dlq.read_errors(limit=3, offset=5)
        assert len(errors) == 3
        assert errors[0]["row"]["id"] == 5

    def test_read_errors_empty_file(self, tmp_path):
        dlq = DeadLetterQueue("load-006", dlq_dir=tmp_path)
        errors = dlq.read_errors()
        assert errors == []

    def test_source_table_defaults_to_collection(self, tmp_path):
        dlq = DeadLetterQueue("load-007", dlq_dir=tmp_path)
        dlq.record_failure("my_col", {"id": 1}, "err")

        errors = dlq.read_errors()
        assert errors[0]["source_table"] == "my_col"

    def test_list_dlq_files_empty(self, tmp_path):
        result = DeadLetterQueue.list_dlq_files(dlq_dir=tmp_path)
        assert result == []

    def test_list_dlq_files(self, tmp_path):
        dlq1 = DeadLetterQueue("load-a", dlq_dir=tmp_path)
        dlq1.record_failure("col", {}, "err")
        dlq2 = DeadLetterQueue("load-b", dlq_dir=tmp_path)
        dlq2.record_failure("col", {}, "err")

        result = DeadLetterQueue.list_dlq_files(dlq_dir=tmp_path)
        assert sorted(result) == ["load-a", "load-b"]

    def test_list_dlq_files_nonexistent_dir(self, tmp_path):
        result = DeadLetterQueue.list_dlq_files(dlq_dir=tmp_path / "does_not_exist")
        assert result == []


class TestPipelineDlqWiring:
    """The streaming pipeline must persist import failures to the DLQ so the
    ``/errors`` endpoint surfaces real data (PRD P5b.3.3)."""

    def _make_pipeline(self, dlq):
        from unittest.mock import MagicMock

        from r2g.streaming.pipeline import StreamingPipeline
        from r2g.types import MappingConfig, Schema

        pipeline = StreamingPipeline.__new__(StreamingPipeline)
        pipeline.dlq = dlq
        pipeline.errors = {}
        pipeline._lock = __import__("threading").Lock()
        pipeline.on_duplicate = "replace"
        return pipeline, MagicMock(spec=MappingConfig), MagicMock(spec=Schema)

    def test_flush_batch_records_import_errors_to_dlq(self, tmp_path):
        from unittest.mock import MagicMock

        from r2g.connectors.arango_writer import ImportBatchError

        dlq = DeadLetterQueue("load-flush", dlq_dir=tmp_path)
        pipeline, _, _ = self._make_pipeline(dlq)

        writer = MagicMock()
        writer.import_batch.side_effect = ImportBatchError(
            collection="users",
            error_count=2,
            total_count=3,
            details=["unique constraint violated: _key 1", "invalid document: _key 2"],
        )

        pipeline._flush_batch(writer, "users", [{"_key": "1"}, {"_key": "2"}])

        entries = dlq.read_errors()
        assert len(entries) == 2
        assert {e["error"] for e in entries} == {
            "unique constraint violated: _key 1",
            "invalid document: _key 2",
        }
        assert all(e["collection"] == "users" for e in entries)

    def test_flush_batch_without_dlq_is_noop(self, tmp_path):
        from unittest.mock import MagicMock

        from r2g.connectors.arango_writer import ImportBatchError

        pipeline, _, _ = self._make_pipeline(None)
        writer = MagicMock()
        writer.import_batch.side_effect = ImportBatchError(
            collection="users", error_count=1, total_count=1, details=["boom"]
        )

        pipeline._flush_batch(writer, "users", [{"_key": "1"}])

        assert pipeline.errors["users"] == ["boom"]
