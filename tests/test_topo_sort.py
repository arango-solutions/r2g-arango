"""Unit tests for the topo_sort module."""
from __future__ import annotations

from r2g.topo_sort import topological_sort_tables
from r2g.types import Column, ForeignKey, Schema, Table


def _schema(**tables_spec) -> Schema:
    tables = {}
    for name, spec in tables_spec.items():
        cols = [
            Column(name=c[0], data_type=c[1], is_nullable=c[2], is_primary_key=c[3])
            for c in spec.get("columns", [])
        ]
        fks = [
            ForeignKey(column=f[0], foreign_table=f[1], foreign_column=f[2])
            for f in spec.get("fks", [])
        ]
        tables[name] = Table(
            name=name,
            columns=cols,
            primary_key=spec.get("pk", []),
            foreign_keys=fks,
        )
    return Schema(tables=tables)


class TestTopologicalSort:
    def test_no_fks_returns_sorted_names(self):
        s = _schema(
            users={"columns": [("id", "integer", False, True)], "pk": ["id"]},
            orders={"columns": [("id", "integer", False, True)], "pk": ["id"]},
        )
        ordered, cycles = topological_sort_tables(s)
        assert cycles == []
        assert set(ordered) == {"users", "orders"}

    def test_simple_dependency_order(self):
        s = _schema(
            users={"columns": [("id", "integer", False, True)], "pk": ["id"]},
            orders={
                "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
                "pk": ["id"],
                "fks": [("user_id", "users", "id")],
            },
        )
        ordered, cycles = topological_sort_tables(s)
        assert cycles == []
        assert ordered.index("users") < ordered.index("orders")

    def test_chain_dependency(self):
        s = _schema(
            countries={"columns": [("id", "integer", False, True)], "pk": ["id"]},
            cities={
                "columns": [("id", "integer", False, True), ("country_id", "integer", False, False)],
                "pk": ["id"],
                "fks": [("country_id", "countries", "id")],
            },
            addresses={
                "columns": [("id", "integer", False, True), ("city_id", "integer", False, False)],
                "pk": ["id"],
                "fks": [("city_id", "cities", "id")],
            },
        )
        ordered, cycles = topological_sort_tables(s)
        assert cycles == []
        assert ordered.index("countries") < ordered.index("cities")
        assert ordered.index("cities") < ordered.index("addresses")

    def test_self_referential_fk_no_cycle(self):
        s = _schema(
            employees={
                "columns": [
                    ("id", "integer", False, True),
                    ("manager_id", "integer", True, False),
                ],
                "pk": ["id"],
                "fks": [("manager_id", "employees", "id")],
            },
        )
        ordered, cycles = topological_sort_tables(s)
        assert cycles == []
        assert "employees" in ordered

    def test_circular_dependency_detected(self):
        s = _schema(
            a={
                "columns": [("id", "integer", False, True), ("b_id", "integer", False, False)],
                "pk": ["id"],
                "fks": [("b_id", "b", "id")],
            },
            b={
                "columns": [("id", "integer", False, True), ("a_id", "integer", False, False)],
                "pk": ["id"],
                "fks": [("a_id", "a", "id")],
            },
        )
        ordered, cycles = topological_sort_tables(s)
        assert len(cycles) > 0
        cycle_tables = set()
        for cycle in cycles:
            cycle_tables.update(cycle)
        assert "a" in cycle_tables
        assert "b" in cycle_tables
        assert set(ordered) == {"a", "b"}

    def test_mixed_cycle_and_non_cycle(self):
        s = _schema(
            root={"columns": [("id", "integer", False, True)], "pk": ["id"]},
            a={
                "columns": [
                    ("id", "integer", False, True),
                    ("root_id", "integer", False, False),
                    ("b_id", "integer", False, False),
                ],
                "pk": ["id"],
                "fks": [("root_id", "root", "id"), ("b_id", "b", "id")],
            },
            b={
                "columns": [("id", "integer", False, True), ("a_id", "integer", False, False)],
                "pk": ["id"],
                "fks": [("a_id", "a", "id")],
            },
        )
        ordered, cycles = topological_sort_tables(s)
        assert len(cycles) > 0
        assert ordered[0] == "root"

    def test_fk_to_nonexistent_table_ignored(self):
        s = _schema(
            orders={
                "columns": [("id", "integer", False, True), ("ext_id", "integer", False, False)],
                "pk": ["id"],
                "fks": [("ext_id", "external_system", "id")],
            },
        )
        ordered, cycles = topological_sort_tables(s)
        assert cycles == []
        assert "orders" in ordered
