from __future__ import annotations

from pathlib import Path

import pytest

from r2g.connectors.csv_source import CsvConnector


@pytest.fixture
def csv_dir(tmp_path: Path) -> Path:
    (tmp_path / "customers.csv").write_text(
        "id,name,age,is_premium,balance\n"
        "1,Alice,30,true,12.50\n"
        "2,Bob,25,false,0.00\n",
        encoding="utf-8",
    )
    (tmp_path / "orders.csv").write_text(
        "id,customer_id,total\n"
        "10,1,99.99\n"
        "11,2,5.00\n",
        encoding="utf-8",
    )
    (tmp_path / "notes.md").write_text("ignore me", encoding="utf-8")
    return tmp_path


class TestCsvGetSchema:
    def test_discovers_one_table_per_file(self, csv_dir):
        schema = CsvConnector(str(csv_dir)).get_schema()
        assert set(schema.tables) == {"customers", "orders"}

    def test_infers_column_types(self, csv_dir):
        schema = CsvConnector(str(csv_dir)).get_schema()
        cols = {c.name: c.data_type for c in schema.tables["customers"].columns}
        assert cols["id"] == "integer"
        assert cols["name"] == "text"
        assert cols["age"] == "integer"
        assert cols["is_premium"] == "boolean"
        assert cols["balance"] == "double precision"

    def test_id_column_becomes_primary_key(self, csv_dir):
        schema = CsvConnector(str(csv_dir)).get_schema()
        customers = schema.tables["customers"]
        assert customers.primary_key == ["id"]
        id_col = next(c for c in customers.columns if c.name == "id")
        assert id_col.is_primary_key is True
        assert id_col.is_nullable is False

    def test_missing_directory_raises(self, tmp_path):
        with pytest.raises(RuntimeError, match="not found"):
            CsvConnector(str(tmp_path / "nope")).get_schema()

    def test_empty_directory_raises(self, tmp_path):
        with pytest.raises(RuntimeError, match="No CSV"):
            CsvConnector(str(tmp_path)).get_schema()


class TestCsvSession:
    def test_count_rows(self, csv_dir):
        with CsvConnector(str(csv_dir)).open_session() as sess:
            assert sess.count_rows("customers") == 2
            assert sess.count_rows("orders") == 2

    def test_stream_rows_yields_typed_dicts(self, csv_dir):
        with CsvConnector(str(csv_dir)).open_session() as sess:
            rows = list(sess.stream_rows("customers"))
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["is_premium"] is True

    def test_dump_table_to_csv(self, csv_dir, tmp_path):
        out = tmp_path / "out" / "customers.csv"
        with CsvConnector(str(csv_dir)).open_session() as sess:
            n = sess.dump_table_to_csv("customers", out)
        assert n == 2
        assert out.exists()
        assert "Alice" in out.read_text(encoding="utf-8")

    def test_unknown_table_raises(self, csv_dir):
        with CsvConnector(str(csv_dir)).open_session() as sess:
            with pytest.raises(RuntimeError, match="No CSV file for table"):
                sess.count_rows("missing")


class TestCsvDelimiter:
    def test_tsv_support(self, tmp_path):
        (tmp_path / "t.tsv").write_text("a\tb\n1\t2\n", encoding="utf-8")
        schema = CsvConnector(str(tmp_path), delimiter="\t").get_schema()
        assert set(c.name for c in schema.tables["t"].columns) == {"a", "b"}
