"""``arango`` — ArangoDB collections: one document collection per entity, one
edge collection per relationship.

This is what r2g's *forward* pipeline would materialize from the relational
shape (``ConfigManager.generate_default_config``): vertex collections named
after the tables, edge collections named ``<from_table>_to_<to_table>`` with
``_from``/``_to`` on the spine ids — so ``arangodb-schema-analyzer`` (ASA)
reads the edges back as relationships between the two entities.

There is no SQL here, so the two text artifacts are reinterpreted (the
:class:`~r2g.forge.core.ForgeArtifacts` field names stay for backward
compatibility):

- ``ddl``      -> a JSON **collection manifest** (:func:`render_manifest`)
  describing collections, the edge projections, and a named graph;
- ``load_sql`` -> a standalone **Python loader script** (python-arango, env
  driven) that reads the manifest and ``forge.rows.json`` beside it and loads
  the database. :func:`project_documents` is the same projection as a library
  call, for callers that already hold the artifacts.

Documents are the canonical rows with ``_key = str(row["id"])``; edges carry
only ``_key``/``_from``/``_to`` so ASA reports no relationship properties.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar, Dict, List

from ..core import SURROGATE_KEY, EdgePlan, Rows, SchemaPlan
from .base import ForgeDialect

MANIFEST_VERSION = 1
GRAPH_NAME = "forge"

#: ArangoDB is schemaless; the "type" a document field takes is whatever JSON
#: value lands in it. Kept so the shared type-table contract holds.
ARANGO_TYPE_FOR_JSON_TYPE: Dict[str, str] = {
    "integer": "number",
    "float": "number",
    "boolean": "bool",
    "string": "string",
}


def edge_projection(edge: EdgePlan) -> Dict[str, Any]:
    """How one edge collection is derived from the from-table's rows."""
    return {
        "name": edge.edge_collection,
        "relationship": edge.relationship,
        "from": {"collection": edge.from_table, "field": SURROGATE_KEY},
        "to": {"collection": edge.to_table, "field": edge.fk_column},
    }


def render_manifest(plan: SchemaPlan) -> Dict[str, Any]:
    """The collection manifest as data (``ddl`` is its JSON serialization)."""
    return {
        "forgeManifestVersion": MANIFEST_VERSION,
        "dialect": ArangoDialect.name,
        "keyField": SURROGATE_KEY,
        "collections": [
            {"name": t.table, "entity": t.entity, "type": "document"} for t in plan.tables
        ],
        "edgeCollections": [edge_projection(e) for e in plan.edges],
        "graph": {
            "name": GRAPH_NAME,
            "edgeDefinitions": [
                {"collection": e.edge_collection, "from": [e.from_table], "to": [e.to_table]}
                for e in plan.edges
            ],
        },
    }


def project_documents(manifest: Dict[str, Any], rows: Rows) -> Dict[str, List[Dict[str, Any]]]:
    """Project canonical rows onto ArangoDB documents and edges per ``manifest``.

    Pure and deterministic; the generated loader script applies the identical
    rule, so the library and script paths cannot drift.
    """
    key_field = manifest["keyField"]
    out: Dict[str, List[Dict[str, Any]]] = {}
    for collection in manifest["collections"]:
        name = collection["name"]
        out[name] = [{"_key": str(row[key_field]), **row} for row in rows[name]]
    for edge in manifest["edgeCollections"]:
        src, dst = edge["from"], edge["to"]
        out[edge["name"]] = [
            {
                "_key": str(row[src["field"]]),
                "_from": f"{src['collection']}/{row[src['field']]}",
                "_to": f"{dst['collection']}/{row[dst['field']]}",
            }
            for row in rows[src["collection"]]
        ]
    return out


_LOADER_TEMPLATE = '''#!/usr/bin/env python3
"""Federation Forge — ArangoDB loader (dialect: arango; seed: {seed}).

Loads forge.collections.json + forge.rows.json (from this script's directory,
or --dir) into an ArangoDB database. Connection comes from the environment:
ARANGO_ENDPOINT (default http://localhost:8529), ARANGO_DB (default _system),
ARANGO_USER (default root), ARANGO_PASSWORD (default empty). Idempotent:
collections are created if missing and documents replaced on duplicate key.
Requires python-arango.
"""

from __future__ import annotations

import argparse
import json
import os
import sys


def project_documents(manifest, rows):
    key_field = manifest["keyField"]
    out = {{}}
    for collection in manifest["collections"]:
        name = collection["name"]
        out[name] = [{{"_key": str(row[key_field]), **row}} for row in rows[name]]
    for edge in manifest["edgeCollections"]:
        src, dst = edge["from"], edge["to"]
        out[edge["name"]] = [
            {{
                "_key": str(row[src["field"]]),
                "_from": f"{{src['collection']}}/{{row[src['field']]}}",
                "_to": f"{{dst['collection']}}/{{row[dst['field']]}}",
            }}
            for row in rows[src["collection"]]
        ]
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", default=os.path.dirname(os.path.abspath(__file__)))
    args = parser.parse_args()

    with open(os.path.join(args.dir, "forge.collections.json"), encoding="utf-8") as fh:
        manifest = json.load(fh)
    with open(os.path.join(args.dir, "forge.rows.json"), encoding="utf-8") as fh:
        rows = json.load(fh)

    from arango import ArangoClient

    client = ArangoClient(hosts=os.environ.get("ARANGO_ENDPOINT", "http://localhost:8529"))
    db = client.db(
        os.environ.get("ARANGO_DB", "_system"),
        username=os.environ.get("ARANGO_USER", "root"),
        password=os.environ.get("ARANGO_PASSWORD", ""),
    )

    for collection in manifest["collections"]:
        if not db.has_collection(collection["name"]):
            db.create_collection(collection["name"])
    for edge in manifest["edgeCollections"]:
        if not db.has_collection(edge["name"]):
            db.create_collection(edge["name"], edge=True)
    graph = manifest.get("graph")
    if graph and graph["edgeDefinitions"] and not db.has_graph(graph["name"]):
        db.create_graph(
            graph["name"],
            edge_definitions=[
                {{
                    "edge_collection": d["collection"],
                    "from_vertex_collections": d["from"],
                    "to_vertex_collections": d["to"],
                }}
                for d in graph["edgeDefinitions"]
            ],
        )

    total = 0
    for name, documents in project_documents(manifest, rows).items():
        result = db.collection(name).import_bulk(documents, on_duplicate="replace")
        errors = result.get("errors", 0)
        if errors:
            print(f"{{name}}: {{errors}} import errors", file=sys.stderr)
            return 1
        total += len(documents)
        print(f"{{name}}: {{len(documents)}} documents")
    print(f"loaded {{total}} documents into {{db.name}}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
'''


class ArangoDialect(ForgeDialect):
    name: ClassVar[str] = "arango"
    ddl_filename: ClassVar[str] = "forge.collections.json"
    loader_filename: ClassVar[str] = "forge.load.py"
    type_for_json: ClassVar[Dict[str, str]] = ARANGO_TYPE_FOR_JSON_TYPE

    def render_ddl(self, plan: SchemaPlan) -> str:
        return json.dumps(render_manifest(plan), indent=2, sort_keys=True) + "\n"

    def render_loader(self, plan: SchemaPlan, rows: Rows, seed: int) -> str:
        return _LOADER_TEMPLATE.format(seed=seed)


__all__ = [
    "ARANGO_TYPE_FOR_JSON_TYPE",
    "GRAPH_NAME",
    "MANIFEST_VERSION",
    "ArangoDialect",
    "edge_projection",
    "project_documents",
    "render_manifest",
]
