"""Topological sort for table import ordering and circular FK detection.

Determines the order in which document collections should be imported so
that edges referencing them can be created safely. Detects circular FK
dependencies and reports them as warnings.
"""
from __future__ import annotations

from r2g.types import Schema


def topological_sort_tables(schema: Schema) -> tuple[list[str], list[list[str]]]:
    """Sort tables in dependency order and detect circular FKs.

    Returns (ordered_tables, cycles) where:
    - ordered_tables: tables sorted so that FK targets appear before
      FK sources (i.e., referenced tables are imported first)
    - cycles: list of circular dependency chains (each chain is a list
      of table names forming a cycle); empty if no cycles exist
    """
    deps: dict[str, set[str]] = {name: set() for name in schema.tables}
    for table_name, table in schema.tables.items():
        for fk in table.foreign_keys:
            if fk.foreign_table in schema.tables and fk.foreign_table != table_name:
                deps[table_name].add(fk.foreign_table)

    # Reverse into "must-come-before" edges: if A depends on B, then B -> A
    graph: dict[str, set[str]] = {name: set() for name in schema.tables}
    in_degree: dict[str, int] = {name: 0 for name in graph}
    for table_name, dep_set in deps.items():
        for dep in dep_set:
            graph[dep].add(table_name)
            in_degree[table_name] += 1

    queue = sorted(name for name, deg in in_degree.items() if deg == 0)
    ordered: list[str] = []

    while queue:
        node = queue.pop(0)
        ordered.append(node)
        for successor in sorted(graph.get(node, set())):
            in_degree[successor] -= 1
            if in_degree[successor] == 0:
                queue.append(successor)

    cycles: list[list[str]] = []
    remaining = set(deps) - set(ordered)
    if remaining:
        cycles = _find_cycles(deps, remaining)
        ordered.extend(sorted(remaining))

    return ordered, cycles


def _find_cycles(
    graph: dict[str, set[str]],
    nodes: set[str],
) -> list[list[str]]:
    """Find all distinct cycles among the given nodes."""
    visited: set[str] = set()
    cycles: list[list[str]] = []

    for start in sorted(nodes):
        if start in visited:
            continue
        path: list[str] = []
        path_set: set[str] = set()
        _dfs_cycle(graph, start, path, path_set, visited, cycles, nodes)

    seen_cycle_sets: list[frozenset[str]] = []
    unique: list[list[str]] = []
    for cycle in cycles:
        cs = frozenset(cycle)
        if cs not in seen_cycle_sets:
            seen_cycle_sets.append(cs)
            unique.append(cycle)

    return unique


def _dfs_cycle(
    graph: dict[str, set[str]],
    node: str,
    path: list[str],
    path_set: set[str],
    visited: set[str],
    cycles: list[list[str]],
    scope: set[str],
) -> None:
    """DFS to find cycles, restricted to nodes in *scope*."""
    if node in path_set:
        idx = path.index(node)
        cycles.append(path[idx:] + [node])
        return
    if node in visited or node not in scope:
        return

    path.append(node)
    path_set.add(node)

    for neighbor in sorted(graph.get(node, set())):
        if neighbor in scope:
            _dfs_cycle(graph, neighbor, path, path_set, visited, cycles, scope)

    path.pop()
    path_set.discard(node)
    visited.add(node)
