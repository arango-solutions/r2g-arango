from __future__ import annotations

import json
import os
import shlex
from datetime import datetime, timezone
from pathlib import Path

from r2g.config import pg_type_to_json_type
from r2g.log import get_logger
from r2g.types import MappingConfig, Schema

logger = get_logger(__name__)

_VALID_ON_DUPLICATE = frozenset({"error", "update", "replace", "ignore"})


def _bash_env_default(var_name: str, value: str) -> str:
    if value == "":
        return f'{var_name}="${{{var_name}:-}}"'
    return f'{var_name}="${{{var_name}:-{shlex.quote(value)}}}"'


class ArangoImportGenerator:
    """Generates arangoimport shell scripts for loading JSONL data into ArangoDB."""

    def __init__(
        self,
        config: MappingConfig,
        endpoint: str = "http://localhost:8529",
        database: str = "_system",
        username: str = "root",
        password: str = "",
        data_dir: str = "./output",
        on_duplicate: str = "replace",
    ) -> None:
        if on_duplicate not in _VALID_ON_DUPLICATE:
            raise ValueError(
                f"on_duplicate must be one of {sorted(_VALID_ON_DUPLICATE)}, got {on_duplicate!r}"
            )
        self.config = config
        self.endpoint = endpoint
        self.database = database
        self.username = username
        self.password = password
        self.data_dir = data_dir
        self.on_duplicate = on_duplicate

    def _build_import_command(
        self,
        collection_name: str,
        file_path: str,
        collection_type: str = "document",
        create_collection: bool = True,
        create_collection_type: str = "",
        overwrite: bool = False,
        threads: int = 4,
    ) -> str:
        """Build a single arangoimport command string."""
        resolved_type = (
            create_collection_type
            or ("edge" if collection_type == "edge" else "document")
        )
        parts: list[str] = [
            "arangoimport",
            "--server.endpoint",
            shlex.quote(self.endpoint),
            "--server.database",
            shlex.quote(self.database),
            "--server.username",
            shlex.quote(self.username),
            "--server.password",
            shlex.quote(self.password),
            "--file",
            shlex.quote(file_path),
            "--type",
            "jsonl",
            "--collection",
            shlex.quote(collection_name),
            "--create-collection",
            "true" if create_collection else "false",
            "--create-collection-type",
            resolved_type,
            "--on-duplicate",
            shlex.quote(self.on_duplicate),
            "--threads",
            str(threads),
        ]
        if overwrite:
            parts.append("--overwrite")
        return " ".join(parts)

    def _build_import_command_for_script(
        self,
        collection_name: str,
        file_path: str,
        collection_type: str = "document",
        create_collection: bool = True,
        create_collection_type: str = "",
        overwrite: bool = False,
        threads: int = 4,
    ) -> str:
        resolved_type = (
            create_collection_type
            or ("edge" if collection_type == "edge" else "document")
        )
        parts: list[str] = [
            "arangoimport",
            "--server.endpoint",
            '"$ARANGO_ENDPOINT"',
            "--server.database",
            '"$ARANGO_DB"',
            "--server.username",
            '"$ARANGO_USER"',
            "--server.password",
            '"$ARANGO_PASSWORD"',
            "--file",
            shlex.quote(file_path),
            "--type",
            "jsonl",
            "--collection",
            shlex.quote(collection_name),
            "--create-collection",
            "true" if create_collection else "false",
            "--create-collection-type",
            resolved_type,
            "--on-duplicate",
            shlex.quote(self.on_duplicate),
            "--threads",
            str(threads),
        ]
        if overwrite:
            parts.append("--overwrite")
        return " ".join(parts)

    def generate_document_commands(self) -> list[str]:
        """Generate import commands for all document collections."""
        base = Path(self.data_dir)
        commands: list[str] = []
        for mapping in self.config.collections.values():
            if mapping.collection_type != "document":
                continue
            file_path = str(base / f"{mapping.target_collection}.jsonl")
            commands.append(
                self._build_import_command(
                    mapping.target_collection,
                    file_path,
                    collection_type="document",
                    create_collection_type="document",
                )
            )
        return commands

    def generate_edge_commands(self) -> list[str]:
        """Generate import commands for all edge collections."""
        base = Path(self.data_dir)
        commands: list[str] = []
        for edge in self.config.edges:
            file_path = str(base / f"{edge.edge_collection}.jsonl")
            commands.append(
                self._build_import_command(
                    edge.edge_collection,
                    file_path,
                    collection_type="edge",
                    create_collection_type="edge",
                )
            )
        return commands

    def generate_script(
        self, output_path: str, overwrite_on_initial: bool = True
    ) -> str:
        """Generate a bash script that imports documents first, then edges.

        Writes the script to ``output_path``, makes it executable, and returns the
        same content. Environment variables ``ARANGO_ENDPOINT``, ``ARANGO_DB``,
        ``ARANGO_USER``, and ``ARANGO_PASSWORD`` override defaults when set.
        """
        generated_at = datetime.now(timezone.utc).isoformat()
        base = Path(self.data_dir)
        lines: list[str] = [
            "#!/usr/bin/env bash",
            "# Generated by R2G ArangoImportGenerator",
            f"# {generated_at}",
            "set -euo pipefail",
            "",
            _bash_env_default("ARANGO_ENDPOINT", self.endpoint),
            _bash_env_default("ARANGO_DB", self.database),
            _bash_env_default("ARANGO_USER", self.username),
            _bash_env_default("ARANGO_PASSWORD", self.password),
            "",
        ]
        for mapping in self.config.collections.values():
            if mapping.collection_type != "document":
                continue
            fp = str(base / f"{mapping.target_collection}.jsonl")
            lines.append(
                "echo "
                + shlex.quote(
                    f"Importing document collection {mapping.target_collection}..."
                )
            )
            lines.append(
                self._build_import_command_for_script(
                    mapping.target_collection,
                    fp,
                    collection_type="document",
                    create_collection_type="document",
                    overwrite=overwrite_on_initial,
                )
            )
            lines.append("")
        for edge in self.config.edges:
            fp = str(base / f"{edge.edge_collection}.jsonl")
            lines.append(
                "echo "
                + shlex.quote(
                    f"Importing edge collection {edge.edge_collection}..."
                )
            )
            lines.append(
                self._build_import_command_for_script(
                    edge.edge_collection,
                    fp,
                    collection_type="edge",
                    create_collection_type="edge",
                    overwrite=overwrite_on_initial,
                )
            )
            lines.append("")
        content = "\n".join(lines).rstrip() + "\n"
        out = Path(output_path)
        try:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(content, encoding="utf-8")
            os.chmod(out, 0o755)
        except OSError as e:
            logger.exception(
                "failed_to_write_arangoimport_script",
                output_path=str(out),
                error=str(e),
            )
            raise
        return content

    def generate_create_graph_aql(self, graph_name: str = "r2g_graph") -> str:
        """Generate arangosh JavaScript to create a named graph from edge definitions."""
        lines = [
            "// Generated for arangosh: create named graph from R2G edge definitions",
            'var graph = require("@arangodb/general-graph");',
            "var edgeDefinitions = [",
        ]
        rel_parts: list[str] = []
        for edge in self.config.edges:
            ec = json.dumps(edge.edge_collection)
            fc = json.dumps(edge.from_collection)
            tc = json.dumps(edge.to_collection)
            rel_parts.append(f"  graph._relation({ec}, [{fc}], [{tc}])")
        lines.append(",\n".join(rel_parts))
        lines.append("];")
        lines.append(f"graph._create({json.dumps(graph_name)}, edgeDefinitions);")
        return "\n".join(lines) + "\n"


_PG_TO_ARANGO_DATATYPE = {
    "integer": "number",
    "float": "number",
    "boolean": "boolean",
}


class CsvImportGenerator:
    """Generates arangoimport commands that work directly on PG CSV dumps.

    Uses --type csv with --translate, --datatype, --from-collection-prefix,
    --to-collection-prefix, and --remove-attribute to let arangoimport handle
    key remapping and edge projection natively -- no intermediate JSONL needed.
    """

    def __init__(
        self,
        config: MappingConfig,
        schema: Schema,
        endpoint: str = "http://localhost:8529",
        database: str = "_system",
        username: str = "root",
        password: str = "",
        data_dir: str = "./dumps",
        on_duplicate: str = "replace",
    ) -> None:
        if on_duplicate not in _VALID_ON_DUPLICATE:
            raise ValueError(
                f"on_duplicate must be one of {sorted(_VALID_ON_DUPLICATE)}, got {on_duplicate!r}"
            )
        self.config = config
        self.schema = schema
        self.endpoint = endpoint
        self.database = database
        self.username = username
        self.password = password
        self.data_dir = data_dir
        self.on_duplicate = on_duplicate

    def _datatype_flags(
        self, table_name: str, exclude_cols: set[str] | None = None,
    ) -> list[str]:
        """Build --datatype flags from the schema's column types.

        Columns in *exclude_cols* are skipped -- use this for PK/FK columns
        that will become _key, _from, or _to (always strings in ArangoDB).

        Nullable columns are also skipped for number/boolean because
        arangoimport cannot coerce an empty CSV value to those types.
        """
        if table_name not in self.schema.tables:
            return []
        skip = exclude_cols or set()
        flags: list[str] = []
        for col in self.schema.tables[table_name].columns:
            if col.name in skip:
                continue
            json_type = pg_type_to_json_type(col.data_type)
            arango_dt = _PG_TO_ARANGO_DATATYPE.get(json_type)
            if arango_dt and not (col.is_nullable and arango_dt in ("number", "boolean")):
                flags.extend(["--datatype", shlex.quote(f"{col.name}={arango_dt}")])
        return flags

    def _csv_file_path(self, table_name: str) -> str:
        return str(Path(self.data_dir) / f"{table_name}.csv")

    def _server_flags_for_script(self) -> list[str]:
        return [
            "--server.endpoint", '"$ARANGO_ENDPOINT"',
            "--server.database", '"$ARANGO_DB"',
            "--server.username", '"$ARANGO_USER"',
            "--server.password", '"$ARANGO_PASSWORD"',
        ]

    def _build_doc_command(self, table_name: str, target_collection: str) -> str:
        """Build arangoimport command for a document collection from a CSV dump."""
        table = self.schema.tables.get(table_name)
        pk_cols = table.primary_key if table else []

        parts: list[str] = [
            "arangoimport",
            *self._server_flags_for_script(),
            "--file", shlex.quote(self._csv_file_path(table_name)),
            "--type", "csv",
            "--collection", shlex.quote(target_collection),
            "--create-collection", "true",
            "--create-collection-type", "document",
            "--on-duplicate", shlex.quote(self.on_duplicate),
        ]

        # PK → _key: force string so auto-detection doesn't treat numeric IDs as numbers
        if len(pk_cols) == 1:
            parts.extend(["--translate", shlex.quote(f"{pk_cols[0]}=_key")])
            parts.extend(["--datatype", shlex.quote(f"{pk_cols[0]}=string")])
        elif len(pk_cols) > 1:
            merge_expr = "[" + "]_[".join(pk_cols) + "]"
            parts.extend([
                "--merge-attributes", shlex.quote(f"_key={merge_expr}"),
            ])
            for pk in pk_cols:
                parts.extend(["--datatype", shlex.quote(f"{pk}=string")])

        parts.extend(self._datatype_flags(table_name, exclude_cols=set(pk_cols)))
        parts.append("--overwrite")
        return " \\\n    ".join(parts)

    def _build_edge_command(
        self,
        table_name: str,
        edge_collection: str,
        from_collection: str,
        to_collection: str,
        from_field: str,
    ) -> str:
        """Build arangoimport command for an edge collection from a CSV dump.

        Uses the source table's PK as _from and the FK column as _to,
        with collection prefixes. All non-structural columns are removed
        so the edge is a clean relationship.
        """
        table = self.schema.tables.get(table_name)
        pk_cols = table.primary_key if table else []
        all_cols = [c.name for c in table.columns] if table else []

        parts: list[str] = [
            "arangoimport",
            *self._server_flags_for_script(),
            "--file", shlex.quote(self._csv_file_path(table_name)),
            "--type", "csv",
            "--collection", shlex.quote(edge_collection),
            "--create-collection", "true",
            "--create-collection-type", "edge",
            "--on-duplicate", shlex.quote(self.on_duplicate),
        ]

        # PK → _from, FK → _to: force string so _id references resolve correctly
        if len(pk_cols) == 1:
            parts.extend(["--translate", shlex.quote(f"{pk_cols[0]}=_from")])
            parts.extend(["--datatype", shlex.quote(f"{pk_cols[0]}=string")])
        elif len(pk_cols) > 1:
            merge_expr = "[" + "]_[".join(pk_cols) + "]"
            parts.extend([
                "--merge-attributes", shlex.quote(f"_from={merge_expr}"),
            ])
            for pk in pk_cols:
                parts.extend(["--datatype", shlex.quote(f"{pk}=string")])

        parts.extend(["--translate", shlex.quote(f"{from_field}=_to")])
        parts.extend(["--datatype", shlex.quote(f"{from_field}=string")])
        parts.extend([
            "--from-collection-prefix", shlex.quote(f"{from_collection}/"),
            "--to-collection-prefix", shlex.quote(f"{to_collection}/"),
        ])

        keep = {"_from", "_to", from_field}
        if len(pk_cols) == 1:
            keep.add(pk_cols[0])
        for col_name in all_cols:
            if col_name not in keep and col_name not in pk_cols:
                parts.extend(["--remove-attribute", shlex.quote(col_name)])

        parts.append("--overwrite")
        return " \\\n    ".join(parts)

    def _build_graph_creation_arangosh(self, graph_name: str) -> list[str]:
        """Build arangosh command to create a named graph."""
        rel_parts: list[str] = []
        for edge in self.config.edges:
            ec = json.dumps(edge.edge_collection)
            fc = json.dumps(edge.from_collection)
            tc = json.dumps(edge.to_collection)
            rel_parts.append(f"  graph._relation({ec}, [{fc}], [{tc}])")

        js_lines = [
            'var graph = require("@arangodb/general-graph");',
            f'try {{ graph._drop({json.dumps(graph_name)}, true); }} catch(e) {{}}',
            "var edgeDefs = [",
            ",\n".join(rel_parts),
            "];",
            f"graph._create({json.dumps(graph_name)}, edgeDefs);",
            f'print("Named graph {graph_name} created.");',
        ]
        js_code = " ".join(js_lines)

        return [
            f"echo 'Creating named graph {graph_name}...'",
            (
                "arangosh"
                ' --server.endpoint "$ARANGO_ENDPOINT"'
                ' --server.database "$ARANGO_DB"'
                ' --server.username "$ARANGO_USER"'
                ' --server.password "$ARANGO_PASSWORD"'
                f" --javascript.execute-string {shlex.quote(js_code)}"
            ),
        ]

    def generate_csv_script(
        self, output_path: str, overwrite_on_initial: bool = True,
        graph_name: str | None = None,
    ) -> str:
        """Generate a bash script that imports PG CSV dumps directly into ArangoDB.

        Documents: --translate pk=_key, --datatype for type coercion
        Edges: --translate pk=_from, --translate fk=_to, collection prefixes,
               --remove-attribute for all non-edge columns

        If *graph_name* is provided, appends a curl call to create the named
        graph via the Gharial API after all imports complete.
        """
        generated_at = datetime.now(timezone.utc).isoformat()

        lines: list[str] = [
            "#!/usr/bin/env bash",
            "# Generated by R2G CsvImportGenerator",
            f"# {generated_at}",
            "# Imports PG CSV dumps directly via arangoimport --type csv",
            "# No intermediate JSONL transformation required",
            "set -euo pipefail",
            "",
            _bash_env_default("ARANGO_ENDPOINT", self.endpoint),
            _bash_env_default("ARANGO_DB", self.database),
            _bash_env_default("ARANGO_USER", self.username),
            _bash_env_default("ARANGO_PASSWORD", self.password),
            "",
            "# === Document collections ===",
            "",
        ]

        for cm in self.config.collections.values():
            if cm.collection_type != "document":
                continue
            lines.append(
                "echo " + shlex.quote(f"Importing document collection {cm.target_collection} from {cm.source_table}.csv...")
            )
            lines.append(self._build_doc_command(cm.source_table, cm.target_collection))
            lines.append("")

        lines.append("# === Edge collections ===")
        lines.append("")

        for edge in self.config.edges:
            lines.append(
                "echo " + shlex.quote(
                    f"Importing edge collection {edge.edge_collection} "
                    f"({edge.from_collection} -> {edge.to_collection}) "
                    f"from {edge.from_collection}.csv..."
                )
            )
            lines.append(self._build_edge_command(
                table_name=edge.from_collection,
                edge_collection=edge.edge_collection,
                from_collection=edge.from_collection,
                to_collection=edge.to_collection,
                from_field=edge.from_field,
            ))
            lines.append("")

        if graph_name:
            lines.append("# === Named graph ===")
            lines.append("")
            lines.extend(self._build_graph_creation_arangosh(graph_name))
            lines.append("")

        content = "\n".join(lines).rstrip() + "\n"
        out = Path(output_path)
        try:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(content, encoding="utf-8")
            os.chmod(out, 0o755)
        except OSError as e:
            logger.exception(
                "failed_to_write_csv_import_script",
                output_path=str(out),
                error=str(e),
            )
            raise
        return content
