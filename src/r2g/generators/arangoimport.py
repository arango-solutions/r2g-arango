from __future__ import annotations

import json
import os
import shlex
from datetime import datetime, timezone
from pathlib import Path

from r2g.log import get_logger
from r2g.types import MappingConfig

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
