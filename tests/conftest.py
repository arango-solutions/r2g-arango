from __future__ import annotations

import pytest

from r2g.types import Column, ForeignKey, Schema, Table

SAMPLE_ROWS = [
    ("1", "Alice", "alice@example.com", "30", "true"),
    ("2", "Bob", "bob@example.com", "25", "true"),
    ("3", "Carol", "carol@example.com", "42", "false"),
    ("4", "Dan", "dan@example.com", "19", "true"),
    ("5", "Eve", "eve@example.com", "55", "false"),
]


@pytest.fixture
def sample_csv(tmp_path):
    path = tmp_path / "sample.csv"
    lines = ["id,name,email,age,active"] + [",".join(row) for row in SAMPLE_ROWS]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


@pytest.fixture
def sample_tsv(tmp_path):
    path = tmp_path / "sample.tsv"
    lines = ["id\tname\temail\tage\tactive"] + ["\t".join(row) for row in SAMPLE_ROWS]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


@pytest.fixture
def sample_schema() -> Schema:
    users = Table(
        name="users",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="name", data_type="text", is_nullable=False),
            Column(name="email", data_type="text", is_nullable=True),
        ],
        primary_key=["id"],
        foreign_keys=[],
    )
    orders = Table(
        name="orders",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="user_id", data_type="integer", is_nullable=False),
            Column(name="total", data_type="numeric", is_nullable=True),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(
                column="user_id",
                foreign_table="users",
                foreign_column="id",
                constraint_name="orders_user_id_fkey",
            ),
        ],
    )
    return Schema(tables={"users": users, "orders": orders})
