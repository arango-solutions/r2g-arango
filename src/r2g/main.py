from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import typer
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax
from rich.table import Table as RichTable

from r2g.config import ConfigManager
from r2g.generators.arangoimport import ArangoImportGenerator, CsvImportGenerator
from r2g.generators.visualizer import MappingVisualizer
from r2g.input.dump_reader import DumpReader
from r2g.log import get_logger, setup_logging
from r2g.transformers.edge_transformer import EdgeTransformer
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import EdgeDefinition, Schema

app = typer.Typer(help="R2G-ETL: Relational to Graph Pipeline")
source_app = typer.Typer(help="Manage data sources")
project_app = typer.Typer(help="Manage projects")
app.add_typer(source_app, name="source")
app.add_typer(project_app, name="project")
console = Console()
log = get_logger(__name__)


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
    json_log: bool = typer.Option(False, "--json-log", help="Output logs as JSON"),
    env_file: Optional[str] = typer.Option(
        None, "--env-file", help="Path to .env file (default: auto-detect .env in cwd)"
    ),
) -> None:
    from dotenv import load_dotenv

    load_dotenv(env_file or ".env", override=False)
    setup_logging(level="DEBUG" if verbose else "INFO", json_output=json_log)


@app.command("validate-data")
def validate_data_cmd(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    dumps_dir: str = typer.Option(..., "--data-dir", help="Directory containing CSV dump files"),
    file_pattern: str = typer.Option("*.csv", "--file-pattern", help="Glob pattern for dump files"),
    max_issues: int = typer.Option(100, "--max-issues", help="Max issues to report per FK relationship"),
) -> None:
    """Validate referential integrity of dump data before import.

    Reads CSV dump files, builds PK lookup sets per table, then checks
    every FK column value to ensure the referenced PK exists in the
    target table's dump. Reports orphaned references that would produce
    broken edges in ArangoDB.
    """
    from r2g.data_validator import validate_data

    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)
        report = validate_data(schema, mapping, dumps_dir, file_pattern, max_issues)

        console.print(
            f"[dim]Scanned {report.rows_scanned:,} rows across "
            f"{report.tables_checked} tables, {report.fk_checks:,} FK checks, "
            f"{report.pk_sets_built} PK sets built[/dim]\n"
        )

        if report.is_clean:
            console.print("[green]Data integrity check passed — no orphaned FK references found.[/green]")
        else:
            summary = report.summary_by_fk()
            table = RichTable(title=f"Orphaned FK References ({len(report.issues)} issues)")
            table.add_column("FK Relationship", style="cyan")
            table.add_column("Orphans", justify="right", style="red")
            for fk_label, count in sorted(summary.items(), key=lambda x: -x[1]):
                table.add_row(fk_label, f"{count:,}")
            console.print(table)

            console.print(f"\n[yellow]Sample orphaned values (first {min(10, len(report.issues))}):[/yellow]")
            for issue in report.issues[:10]:
                console.print(
                    f"  [red]![/red] {issue.source_table}.{issue.fk_column} "
                    f"row {issue.row_number}: value '{issue.orphan_value}' "
                    f"not in {issue.target_table} PKs"
                )
            if len(report.issues) > 10:
                console.print(f"  [dim]... and {len(report.issues) - 10} more[/dim]")

            raise typer.Exit(code=1)

    except typer.Exit:
        raise
    except Exception as e:
        log.exception("validate_data_failed")
        console.print(f"[red]Data validation failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("validate-config")
def validate_config_cmd(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
) -> None:
    """Validate a mapping config against a schema file.

    Checks that every collection references a known table, every edge
    references valid collections and columns, and field lists only name
    columns that exist in the source table.
    """
    from r2g.config import validate_config

    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)
        issues = validate_config(schema, mapping)
        if issues:
            console.print(f"[red]Found {len(issues)} issue(s):[/red]")
            for issue in issues:
                console.print(f"  [yellow]•[/yellow] {issue}")
            raise typer.Exit(code=1)
        n_coll = len(mapping.collections)
        n_edge = len(mapping.edges)
        console.print(
            f"[green]Config valid![/green] "
            f"{n_coll} collections, {n_edge} edges — all references resolve."
        )
    except typer.Exit:
        raise
    except Exception as e:
        log.exception("validate_config_failed")
        console.print(f"[red]Validation failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("generate-config")
def generate_config(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    output: str = typer.Option("mapping.yaml", "--output", "-o", help="Output YAML path"),
) -> None:
    """Generate a default mapping config from a schema file."""
    try:
        schema = Schema.load_from_file(schema_file)
        config = ConfigManager.generate_default_config(schema)
        ConfigManager.save_config(config, output)
        n_collections = len(config.collections)
        n_edges = len(config.edges)
        console.print(
            f"[green]Wrote mapping config to[/green] [bold]{output}[/bold] "
            f"([bold]{n_collections}[/bold] collections, [bold]{n_edges}[/bold] edges)."
        )
    except Exception as e:
        log.exception("generate_config_failed", output=output)
        console.print(f"[red]Failed to generate config:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("visualize-mapping")
def visualize_mapping(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    output: str = typer.Option("mapping_viz.html", "--output", "-o", help="Output HTML file"),
    no_open: bool = typer.Option(False, "--no-open", help="Do not open in default browser"),
) -> None:
    """Generate an interactive HTML visualization of the PG-to-graph mapping."""
    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)
        viz = MappingVisualizer(schema, mapping)
        viz.generate(output)
        console.print(f"[green]Wrote mapping visualization:[/green] [bold]{output}[/bold]")
        console.print(
            f"  [dim]{len(schema.tables)} tables → "
            f"{len(mapping.collections)} collections, "
            f"{len(mapping.edges)} edges[/dim]"
        )
        if not no_open:
            import webbrowser
            webbrowser.open(f"file://{Path(output).resolve()}")
    except Exception as e:
        log.exception("visualize_mapping_failed", output=output)
        console.print(f"[red]Failed to generate visualization:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("transform-edges")
def transform_edges(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    table_name: str = typer.Option(..., "--table", "-t", help="Source table name"),
    dump_file: str = typer.Option(..., "--input", "-i", help="Input dump file"),
    output_file: str = typer.Option(..., "--output", "-o", help="Output JSONL file (base name if multiple edges)"),
    limit: Optional[int] = typer.Option(None, help="Max rows to process"),
) -> None:
    """Transform a table dump into ArangoDB edge documents (JSONL)."""
    try:
        schema = Schema.load_from_file(schema_file)
        if table_name not in schema.tables:
            raise ValueError(f"Table '{table_name}' not found in schema.")
        table_def = schema.tables[table_name]

        mapping = ConfigManager.load_config(config_path)
        matching: list[EdgeDefinition] = [e for e in mapping.edges if e.from_collection == table_name]
        if not matching:
            raise ValueError(
                f"No edge definitions with from_collection='{table_name}' in mapping config."
            )

        reader = DumpReader(dump_file)
        out_path = Path(output_file)
        transformers = [
            (e, EdgeTransformer(e, table_def, key_separator=mapping.key_separator))
            for e in matching
        ]

        if len(matching) == 1:
            out_paths = [(matching[0], out_path)]
        else:
            base = out_path.stem
            parent = out_path.parent
            suffix = out_path.suffix if out_path.suffix else ".jsonl"
            out_paths = [
                (e, parent / f"{base}_{e.edge_collection}{suffix}") for e in matching
            ]

        handles: list[tuple[EdgeDefinition, Any]] = []
        try:
            for e, path in out_paths:
                path.parent.mkdir(parents=True, exist_ok=True)
                handles.append((e, path.open("w", encoding="utf-8")))

            handle_map = {e.edge_collection: h for e, h in handles}
            counts: dict[str, int] = {e.edge_collection: 0 for e in matching}
            row_num = 0
            for row in reader.read_rows():
                for edge_def, transformer in transformers:
                    doc = transformer.transform_row(row)
                    if doc is not None:
                        handle_map[edge_def.edge_collection].write(json.dumps(doc) + "\n")
                        counts[edge_def.edge_collection] += 1
                row_num += 1
                if limit is not None and row_num >= limit:
                    break
        finally:
            for _, h in handles:
                h.close()

        for e, path in out_paths:
            console.print(
                f"[green]Wrote[/green] [bold]{counts[e.edge_collection]}[/bold] edges "
                f"to [bold]{path}[/bold] ([cyan]{e.edge_collection}[/cyan])."
            )
    except Exception as e:
        log.exception("transform_edges_failed", table=table_name)
        console.print(f"[red]Edge transformation failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("transform-all")
def transform_all(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    input_dir: str = typer.Option(..., "--input-dir", help="Directory of dump files (one per table)"),
    output_dir: str = typer.Option("./output", "--output-dir", help="Output directory for JSONL files"),
    file_pattern: str = typer.Option("*.csv", "--file-pattern", help="Glob pattern for input files"),
) -> None:
    """Transform an entire dataset: node JSONL per table, then edge JSONL per edge definition."""
    summary: list[tuple[str, str, int]] = []

    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)
        in_dir = Path(input_dir)
        out_root = Path(output_dir)
        out_root.mkdir(parents=True, exist_ok=True)

        if not in_dir.is_dir():
            raise ValueError(f"Input directory does not exist or is not a directory: {input_dir}")

        dump_by_table: dict[str, Path] = {}
        for fpath in sorted(in_dir.glob(file_pattern)):
            if fpath.is_file():
                dump_by_table[fpath.stem] = fpath

        doc_jobs: list[tuple[str, str, Path, Any, Any]] = []
        for _key, cm in mapping.collections.items():
            if cm.collection_type != "document":
                continue
            st = cm.source_table
            if st not in schema.tables:
                log.warning("transform_all_skip_unknown_table", source_table=st)
                continue
            if st not in dump_by_table:
                log.warning("transform_all_skip_no_dump", source_table=st)
                continue
            doc_jobs.append((st, cm.target_collection, dump_by_table[st], schema.tables[st], cm))

        edge_jobs: list[tuple[EdgeDefinition, Path, Any]] = []
        for edge in mapping.edges:
            src = edge.from_collection
            if src not in schema.tables:
                log.warning("transform_all_edge_unknown_table", from_collection=src)
                continue
            if src not in dump_by_table:
                log.warning("transform_all_edge_no_dump", from_collection=src)
                continue
            edge_jobs.append((edge, dump_by_table[src], schema.tables[src]))

        total_steps = len(doc_jobs) + len(edge_jobs)
        if total_steps == 0:
            console.print(
                "[yellow]No document or edge transforms to run "
                "(check mapping config, schema, and input files).[/yellow]"
            )
            return

        progress_cols = (
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
        )

        with Progress(*progress_cols, console=console) as progress:
            task_id = progress.add_task("Transforming dataset", total=total_steps)

            for table_name, target_coll, dump_path, table_def, col_mapping in doc_jobs:
                progress.update(task_id, description=f"Nodes: {target_coll}")
                out_file = out_root / f"{target_coll}.jsonl"
                transformer = NodeTransformer(
                    table_def,
                    collection_mapping=col_mapping,
                    key_separator=mapping.key_separator,
                    type_overrides=mapping.type_overrides,
                )
                reader = DumpReader(str(dump_path))
                n = 0
                with out_file.open("w", encoding="utf-8") as f_out:
                    for row in reader.read_rows():
                        doc = transformer.transform_row(row)
                        f_out.write(json.dumps(doc) + "\n")
                        n += 1
                summary.append((target_coll, "document", n))
                progress.advance(task_id)

            for edge, dump_path, table_def in edge_jobs:
                progress.update(task_id, description=f"Edges: {edge.edge_collection}")
                out_file = out_root / f"{edge.edge_collection}.jsonl"
                et = EdgeTransformer(edge, table_def, key_separator=mapping.key_separator)
                reader = DumpReader(str(dump_path))
                n = 0
                with out_file.open("w", encoding="utf-8") as f_out:
                    for row in reader.read_rows():
                        doc = et.transform_row(row)
                        if doc is not None:
                            f_out.write(json.dumps(doc) + "\n")
                            n += 1
                summary.append((edge.edge_collection, "edge", n))
                progress.advance(task_id)

        table = RichTable(title="Transform summary")
        table.add_column("Collection", style="cyan")
        table.add_column("Type", style="magenta")
        table.add_column("Rows", justify="right")
        for name, kind, rows in summary:
            table.add_row(name, kind, str(rows))
        console.print(table)
        console.print("[green]Transform complete.[/green]")
    except Exception as e:
        log.exception("transform_all_failed")
        console.print(f"[red]Full transform failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("generate-import")
def generate_import(
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    data_dir: str = typer.Option(..., "--data-dir", help="Directory containing JSONL files"),
    output: str = typer.Option("import.sh", "--output", "-o", help="Output shell script path"),
    endpoint: str = typer.Option(
        "http://127.0.0.1:8529", "--endpoint", help="ArangoDB endpoint URL", envvar="ARANGO_ENDPOINT"
    ),
    database: str = typer.Option("_system", "--database", "-d", help="Database name", envvar="ARANGO_DB"),
    username: str = typer.Option("root", "--username", "-u", help="ArangoDB username", envvar="ARANGO_USER"),
    password: str = typer.Option("", "--password", "-p", help="ArangoDB password", envvar="ARANGO_PASSWORD"),
    on_duplicate: str = typer.Option("ignore", "--on-duplicate", help="arangoimport --on-duplicate value"),
    graph_name: Optional[str] = typer.Option(None, "--graph-name", help="If set, also write a graph creation AQL file"),
) -> None:
    """Generate an arangoimport shell script (and optional arangosh graph creation script)."""
    try:
        mapping = ConfigManager.load_config(config_path)
        gen = ArangoImportGenerator(
            mapping,
            endpoint,
            database,
            username,
            password,
            data_dir,
            on_duplicate,
        )
        gen.generate_script(output, overwrite_on_initial=True)
        console.print(f"[green]Wrote import script:[/green] [bold]{output}[/bold]")
        if graph_name:
            graph_script = gen.generate_create_graph_aql(graph_name)
            out_p = Path(output)
            graph_path = out_p.with_name(f"{out_p.stem}_graph_{graph_name}.js")
            graph_path.write_text(graph_script, encoding="utf-8")
            console.print(
                f"[green]Wrote arangosh graph creation script:[/green] [bold]{graph_path}[/bold]"
            )
    except Exception as e:
        log.exception("generate_import_failed", output=output)
        console.print(f"[red]Failed to generate import script:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("generate-csv-import")
def generate_csv_import(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    data_dir: str = typer.Option(..., "--data-dir", help="Directory containing PG CSV dump files"),
    output: str = typer.Option("import_csv.sh", "--output", "-o", help="Output shell script path"),
    endpoint: str = typer.Option(
        "http://127.0.0.1:8529", "--endpoint", help="ArangoDB endpoint URL", envvar="ARANGO_ENDPOINT"
    ),
    database: str = typer.Option("_system", "--database", "-d", help="Database name", envvar="ARANGO_DB"),
    username: str = typer.Option("root", "--username", "-u", help="ArangoDB username", envvar="ARANGO_USER"),
    password: str = typer.Option("", "--password", "-p", help="ArangoDB password", envvar="ARANGO_PASSWORD"),
    on_duplicate: str = typer.Option("replace", "--on-duplicate", help="arangoimport --on-duplicate value"),
    graph_name: Optional[str] = typer.Option(
        None, "--graph-name", help="Create a named graph after import via Gharial API"
    ),
) -> None:
    """Generate an arangoimport script that imports PG CSV dumps directly.

    Uses arangoimport --type csv with --translate for key remapping,
    --datatype for type coercion, --from-collection-prefix / --to-collection-prefix
    for edge generation, and --remove-attribute for column projection.
    No intermediate JSONL transformation required.
    """
    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)
        gen = CsvImportGenerator(
            mapping,
            schema,
            endpoint,
            database,
            username,
            password,
            data_dir,
            on_duplicate,
        )
        gen.generate_csv_script(output, overwrite_on_initial=True, graph_name=graph_name)
        console.print(f"[green]Wrote CSV import script:[/green] [bold]{output}[/bold]")
        console.print(
            f"  [dim]{len(mapping.collections)} document collections, "
            f"{len(mapping.edges)} edge collections[/dim]"
        )
        if graph_name:
            console.print(f"  [dim]Named graph '{graph_name}' will be created after import[/dim]")
        console.print(
            "  [dim]Imports PG CSV dumps directly — no JSONL transformation needed[/dim]"
        )
    except Exception as e:
        log.exception("generate_csv_import_failed", output=output)
        console.print(f"[red]Failed to generate CSV import script:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def transform_nodes(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    table_name: str = typer.Option(..., "--table", "-t", help="Table name in the schema"),
    dump_file: str = typer.Option(..., "--input", "-i", help="Input dump file"),
    output_file: str = typer.Option(..., "--output", "-o", help="Output JSONL file"),
    config_path: Optional[str] = typer.Option(
        None, "--config", "-c", help="Mapping config YAML (enables type coercion)"
    ),
    limit: Optional[int] = typer.Option(None, help="Max rows to process"),
) -> None:
    """Transform a table dump into ArangoDB node documents (JSONL)."""
    try:
        schema = Schema.load_from_file(schema_file)
        if table_name not in schema.tables:
            raise ValueError(f"Table '{table_name}' not found in schema.")

        table_def = schema.tables[table_name]

        col_mapping = None
        key_sep = "_"
        type_ovr: dict[str, str] = {}
        if config_path:
            mapping = ConfigManager.load_config(config_path)
            col_mapping = mapping.collections.get(table_name)
            key_sep = mapping.key_separator
            type_ovr = mapping.type_overrides

        transformer = NodeTransformer(
            table_def,
            collection_mapping=col_mapping,
            key_separator=key_sep,
            type_overrides=type_ovr,
        )
        reader = DumpReader(dump_file)

        console.print(f"[green]Transforming nodes for table '{table_name}'...[/green]")

        count = 0
        with open(output_file, "w", encoding="utf-8") as f_out:
            for row in reader.read_rows():
                doc = transformer.transform_row(row)

                f_out.write(json.dumps(doc) + "\n")

                count += 1
                if limit is not None and count >= limit:
                    break

        console.print(f"Successfully wrote {count} documents to [bold]{output_file}[/bold]")
    except Exception as e:
        log.exception("transform_nodes_failed", table=table_name)
        console.print(f"[red]Transformation failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def inspect_dump(
    file_path: str = typer.Argument(..., help="Path to the dump file (CSV/TSV/GZ)"),
    delimiter: str = typer.Option(",", help="Delimiter character"),
    limit: int = typer.Option(5, help="Number of rows to preview"),
) -> None:
    """Preview the contents of a dump file to verify parsing."""
    console.print(f"[green]Inspecting {file_path}...[/green]")

    try:
        reader = DumpReader(file_path, delimiter=delimiter)
        count = 0
        for row in reader.read_rows():
            if count == 0:
                console.print(f"[bold]Columns detected:[/bold] {list(row.keys())}")

            console.print(row)
            count += 1
            if count >= limit:
                break

    except Exception as e:
        log.exception("inspect_dump_failed", file_path=file_path)
        console.print(f"[red]Failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def ingest_schema(
    connection_string: str = typer.Option(..., "--conn", "-c", help="PostgreSQL connection string", envvar="PG_CONN"),
    output: str = typer.Option("./schema.json", "--output", "-o", help="Path to save the schema metadata"),
    pg_schema: str = typer.Option("public", "--pg-schema", help="PostgreSQL schema name to introspect"),
) -> None:
    """Connect to PostgreSQL and extract schema metadata."""
    from r2g.connectors.postgres import PostgresConnector

    console.print("[green]Connecting to PostgreSQL...[/green]")

    try:
        connector = PostgresConnector(connection_string, schema_name=pg_schema)
        schema = connector.get_schema()

        console.print(f"[green]Successfully extracted schema with {len(schema.tables)} tables.[/green]")

        schema.save_to_file(output)
        console.print(f"Schema metadata saved to [bold]{output}[/bold]")

    except Exception as e:
        log.exception("ingest_schema_failed")
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def validate_schema(
    schema_file: str = typer.Argument(..., help="Path to the schema JSON file"),
) -> None:
    """Validate a schema file against the internal model."""
    try:
        schema = Schema.load_from_file(schema_file)
        console.print("[green]Schema valid![/green]")
        console.print(f"Contains {len(schema.tables)} tables.")

        json_str = schema.model_dump_json(indent=2)
        syntax = Syntax(json_str, "json", theme="monokai", line_numbers=True)
        console.print(syntax)

    except Exception as e:
        log.exception("validate_schema_failed", schema_file=schema_file)
        console.print(f"[red]Invalid schema file:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("stream")
def stream(
    pg_conn: Optional[str] = typer.Option(
        None,
        "--pg-conn",
        help="PostgreSQL connection string (legacy; prefer --source)",
        envvar="PG_CONN",
    ),
    source_name: Optional[str] = typer.Option(
        None,
        "--source",
        help="Catalog source name; resolves to any supported source_type (PostgreSQL, Snowflake, …)",
    ),
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    endpoint: str = typer.Option(
        "http://127.0.0.1:8529", "--endpoint", help="ArangoDB endpoint URL", envvar="ARANGO_ENDPOINT"
    ),
    database: str = typer.Option("_system", "--database", "-d", help="ArangoDB database name", envvar="ARANGO_DB"),
    username: str = typer.Option("root", "--username", "-u", help="ArangoDB username", envvar="ARANGO_USER"),
    password: str = typer.Option("", "--password", "-p", help="ArangoDB password", envvar="ARANGO_PASSWORD"),
    batch_size: int = typer.Option(10000, "--batch-size", "-b", help="Rows per batch"),
    on_duplicate: str = typer.Option("replace", "--on-duplicate", help="ArangoDB on-duplicate strategy"),
    graph_name: Optional[str] = typer.Option(None, "--graph-name", help="Create a named graph after import"),
    pg_schema: str = typer.Option("public", "--pg-schema", help="PostgreSQL schema name to stream from"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Validate connections and preview data without writing to ArangoDB"
    ),
    drop_collections: bool = typer.Option(
        False, "--drop-collections", help="Drop and recreate target collections before import"
    ),
    workers: int = typer.Option(
        1, "--workers", "-w", help="Parallel workers (each gets its own PG + ArangoDB connection)"
    ),
    include_tables: Optional[str] = typer.Option(
        None, "--include-tables", help="Comma-separated list of tables to include (default: all)"
    ),
    exclude_tables: Optional[str] = typer.Option(
        None, "--exclude-tables", help="Comma-separated list of tables to exclude"
    ),
    skip_existing: bool = typer.Option(
        False, "--skip-existing", help="Skip collections that already contain data (for resuming partial runs)"
    ),
    since: Optional[str] = typer.Option(
        None, "--since",
        help="Only stream rows where the timestamp column >= this value (ISO 8601, e.g. 2026-04-01T00:00:00)"
    ),
    since_column: Optional[str] = typer.Option(
        None, "--since-column",
        help="Column to use for --since filtering (default: auto-detect updated_at/created_at)"
    ),
) -> None:
    """Stream data directly from a relational source to ArangoDB (no intermediate files).

    Opens a consistent-snapshot read session on the source (PG
    ``REPEATABLE READ`` / Snowflake ``BEGIN``), reads tables in
    configurable batches, transforms rows on the fly, and bulk-imports
    into ArangoDB via the HTTP API.

    Use ``--source <name>`` to dispatch by catalog ``source_type``
    (PostgreSQL, Snowflake, …). The legacy ``--pg-conn`` flag still
    works and routes through the PostgreSQL connector.

    Use --dry-run to preview row counts and sample documents without writing.
    Use --since with --on-duplicate=replace for basic incremental updates.
    """
    from r2g.connectors.arango_writer import ArangoWriter
    from r2g.connectors.base import create_source_connector
    from r2g.streaming.pipeline import StreamingPipeline

    if not pg_conn and not source_name:
        console.print("[red]Specify either --source <name> or --pg-conn <url>.[/red]")
        raise typer.Exit(code=2)

    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)

        writer = ArangoWriter(
            endpoint=endpoint,
            database=database,
            username=username,
            password=password,
        )

        inc = {t.strip() for t in include_tables.split(",")} if include_tables else None
        exc = {t.strip() for t in exclude_tables.split(",")} if exclude_tables else None

        if source_name:
            mgr = _get_catalog()
            source = mgr.get_source(source_name)
            if source is None:
                console.print(f"[red]Source '{source_name}' not found in catalog.[/red]")
                raise typer.Exit(code=1)
            source_connector = create_source_connector(
                source.source_type or "postgresql",
                source.connection_string,
                schema_name=pg_schema,
                source_params=source.source_params,
            )
            source_label = f"{source.source_type} ({source_name})"
        else:
            from r2g.connectors.postgres import PostgresConnector

            source_connector = PostgresConnector(pg_conn, schema_name=pg_schema)
            source_label = pg_conn.split("@")[-1] if pg_conn and "@" in pg_conn else pg_conn

        pipeline = StreamingPipeline(
            source_connector=source_connector,
            arango_writer=writer,
            schema=schema,
            config=mapping,
            batch_size=batch_size,
            on_duplicate=on_duplicate,
            pg_schema=pg_schema,
            dry_run=dry_run,
            drop_collections=drop_collections,
            workers=workers,
            include_tables=inc,
            exclude_tables=exc,
            skip_existing=skip_existing,
            since=since,
            since_column=since_column,
        )

        mode_label = "[yellow]DRY RUN[/yellow] — " if dry_run else ""
        console.print(
            f"{mode_label}[green]Streaming from source → ArangoDB[/green]\n"
            f"  Source: {source_label}\n"
            f"  ArangoDB: {endpoint}/{database}\n"
            f"  Batch size: {batch_size:,}"
        )

        from rich.progress import (
            BarColumn as PBarColumn,
        )
        from rich.progress import (
            MofNCompleteColumn as PMofN,
        )
        from rich.progress import (
            SpinnerColumn as PSpinner,
        )
        from rich.progress import (
            TextColumn as PText,
        )
        from rich.progress import (
            TimeElapsedColumn as PTime,
        )

        progress_tasks: dict[str, Any] = {}
        progress_ctx = Progress(
            PSpinner(),
            PText("[progress.description]{task.description}"),
            PBarColumn(bar_width=30),
            PMofN(),
            PText("rows"),
            PTime(),
            console=console,
            transient=True,
        )

        def on_progress(event: str, name: str, current: int, total: int | None) -> None:
            if event == "start":
                progress_tasks[name] = progress_ctx.add_task(name, total=total or 0)
            elif event == "progress":
                progress_ctx.update(progress_tasks[name], completed=current)
            elif event == "done":
                progress_ctx.update(progress_tasks[name], completed=total or current)

        with progress_ctx:
            results = pipeline.run(graph_name=graph_name, on_progress=on_progress)

        title = "Dry Run Preview" if dry_run else "Streaming Import Summary"
        table = RichTable(title=title)
        table.add_column("Collection", style="cyan")
        table.add_column("Type", style="magenta")
        table.add_column("Rows", justify="right")
        for name, count in results["documents"]:
            table.add_row(name, "document", f"{count:,}")
        for name, count in results["edges"]:
            table.add_row(name, "edge", f"{count:,}")
        console.print(table)

        total_docs = sum(c for _, c in results["documents"])
        total_edges = sum(c for _, c in results["edges"])
        total_rows = total_docs + total_edges
        elapsed = results.get("elapsed_seconds", 0)
        throughput = total_rows / elapsed if elapsed > 0 else 0

        skipped = results.get("skipped", [])
        if skipped:
            console.print(
                f"[yellow]Skipped {len(skipped)} existing collection(s):[/yellow] "
                + ", ".join(skipped)
            )

        import_errors = results.get("errors", {})
        if import_errors:
            err_table = RichTable(title="Import Errors", style="red")
            err_table.add_column("Collection", style="cyan")
            err_table.add_column("Errors", justify="right", style="red")
            err_table.add_column("Sample Details")
            for coll_name, details in import_errors.items():
                sample = details[0] if details else ""
                err_table.add_row(coll_name, str(len(details)), sample)
            console.print(err_table)

        if dry_run:
            console.print(
                f"[yellow]Dry run complete:[/yellow] {total_docs:,} documents, "
                f"{total_edges:,} edges [bold]would be[/bold] imported "
                f"[dim]({elapsed:.1f}s, {throughput:,.0f} rows/s)[/dim]"
            )
            if pipeline.previews:
                console.print("\n[bold]Sample documents:[/bold]")
                for coll_name, samples in pipeline.previews.items():
                    if not samples:
                        continue
                    console.print(f"\n  [cyan]{coll_name}[/cyan] (first {len(samples)}):")
                    for doc in samples:
                        console.print(f"    {json.dumps(doc, default=str)}")
            if graph_name:
                console.print(f"\n[yellow]Named graph '{graph_name}' would be created.[/yellow]")
        else:
            console.print(
                f"[green]Stream complete:[/green] {total_docs:,} documents, "
                f"{total_edges:,} edges imported "
                f"[dim]({elapsed:.1f}s, {throughput:,.0f} rows/s)[/dim]"
            )
            if graph_name:
                console.print(f"[green]Named graph '{graph_name}' created.[/green]")

    except Exception as e:
        log.exception("stream_failed")
        console.print(f"[red]Streaming pipeline failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("diff-schema")
def diff_schema(
    old_schema: str = typer.Option(..., "--old", help="Path to the old schema.json"),
    new_schema: str = typer.Option(..., "--new", help="Path to the new schema.json"),
    json_output: bool = typer.Option(False, "--json", help="Output diff as JSON"),
) -> None:
    """Compare two schema files and report structural changes.

    Detects added/removed tables, added/removed/changed columns,
    primary key changes, and foreign key changes.
    """
    from r2g.schema_diff import diff_schemas

    try:
        old = Schema.load_from_file(old_schema)
        new = Schema.load_from_file(new_schema)
        result = diff_schemas(old, new)

        if json_output:
            console.print(json.dumps(result, indent=2))
            return

        has_changes = False

        if result["added_tables"]:
            has_changes = True
            console.print("[green]Added tables:[/green]")
            for t in result["added_tables"]:
                console.print(f"  [green]+[/green] {t}")

        if result["removed_tables"]:
            has_changes = True
            console.print("[red]Removed tables:[/red]")
            for t in result["removed_tables"]:
                console.print(f"  [red]-[/red] {t}")

        if result["modified_tables"]:
            has_changes = True
            for table_name, changes in result["modified_tables"].items():
                console.print(f"\n[yellow]Modified table:[/yellow] [bold]{table_name}[/bold]")

                for col in changes.get("added_columns", []):
                    console.print(f"  [green]+[/green] column [cyan]{col['name']}[/cyan] ({col['type']})")

                for col in changes.get("removed_columns", []):
                    console.print(f"  [red]-[/red] column [cyan]{col}[/cyan]")

                for change in changes.get("type_changes", []):
                    console.print(
                        f"  [yellow]~[/yellow] column [cyan]{change['column']}[/cyan]: "
                        f"{change['old_type']} → {change['new_type']}"
                    )

                for change in changes.get("nullable_changes", []):
                    label = "nullable" if change["new_nullable"] else "NOT NULL"
                    console.print(
                        f"  [yellow]~[/yellow] column [cyan]{change['column']}[/cyan]: → {label}"
                    )

                if changes.get("pk_changed"):
                    console.print(
                        f"  [yellow]~[/yellow] primary key: "
                        f"{changes['old_pk']} → {changes['new_pk']}"
                    )

                for fk in changes.get("added_fks", []):
                    console.print(
                        f"  [green]+[/green] FK {fk['columns']} → "
                        f"{fk['foreign_table']}.{fk['foreign_columns']}"
                    )

                for fk in changes.get("removed_fks", []):
                    console.print(
                        f"  [red]-[/red] FK {fk['columns']} → "
                        f"{fk['foreign_table']}.{fk['foreign_columns']}"
                    )

        if not has_changes:
            console.print("[green]Schemas are identical — no changes detected.[/green]")

    except Exception as e:
        log.exception("diff_schema_failed")
        console.print(f"[red]Schema diff failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("migrate-config")
def migrate_config_cmd(
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to the NEW schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Existing mapping config YAML"),
    output: Optional[str] = typer.Option(
        None, "--output", "-o", help="Output path for updated config (default: overwrite input)"
    ),
    source_schema: Optional[str] = typer.Option(None, "--source-schema", help="Override source_schema in config"),
    json_report: bool = typer.Option(False, "--json-report", help="Output migration report as JSON"),
) -> None:
    """Migrate a mapping config to match an updated PostgreSQL schema.

    Preserves user customizations (collection renames, field mappings,
    include/exclude lists, type overrides) while adding mappings for new
    tables and edges, removing edges for dropped FKs, and flagging
    orphaned collections whose source table no longer exists.
    """
    from r2g.config_migrate import migrate_config

    try:
        new_schema = Schema.load_from_file(schema_file)
        old_config = ConfigManager.load_config(config_path)
        updated, report = migrate_config(old_config, new_schema, source_schema=source_schema)

        out_path = output or config_path
        ConfigManager.save_config(updated, out_path)

        if json_report:
            typer.echo(json.dumps({
                "added_collections": report.added_collections,
                "orphaned_collections": report.orphaned_collections,
                "added_edges": report.added_edges,
                "removed_edges": report.removed_edges,
                "cleaned_fields": report.cleaned_fields,
                "output": out_path,
            }, indent=2))
            return

        if not report.has_changes:
            console.print("[green]Config is already up to date — no migration needed.[/green]")
        else:
            if report.added_collections:
                console.print("[green]Added collections:[/green]")
                for name in report.added_collections:
                    console.print(f"  [green]+[/green] {name}")
            if report.orphaned_collections:
                console.print("[yellow]Orphaned collections (source table removed):[/yellow]")
                for name in report.orphaned_collections:
                    console.print(f"  [yellow]![/yellow] {name}")
            if report.added_edges:
                console.print("[green]Added edges:[/green]")
                for name in report.added_edges:
                    console.print(f"  [green]+[/green] {name}")
            if report.removed_edges:
                console.print("[red]Removed edges (FK dropped):[/red]")
                for name in report.removed_edges:
                    console.print(f"  [red]-[/red] {name}")
            if report.cleaned_fields:
                console.print("[yellow]Cleaned references:[/yellow]")
                for note in report.cleaned_fields:
                    console.print(f"  [yellow]~[/yellow] {note}")

        console.print(
            f"\n[green]Wrote updated config to[/green] [bold]{out_path}[/bold] "
            f"([bold]{len(updated.collections)}[/bold] collections, "
            f"[bold]{len(updated.edges)}[/bold] edges)."
        )

    except Exception as e:
        log.exception("migrate_config_failed")
        console.print(f"[red]Config migration failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("dump-tables")
def dump_tables(
    connection_string: str = typer.Option(..., "--conn", "-c", help="PostgreSQL connection string", envvar="PG_CONN"),
    output_dir: str = typer.Option("./dumps", "--output-dir", "-o", help="Directory to write CSV files"),
    schema_filter: Optional[str] = typer.Option("public", "--schema", help="PostgreSQL schema to dump"),
    tables: Optional[str] = typer.Option(None, "--tables", "-t", help="Comma-separated list of tables (default: all)"),
) -> None:
    """Connect to PostgreSQL and dump each table to a CSV file.

    Runs COPY <table> TO STDOUT WITH CSV HEADER for every table in the schema
    (or a subset if --tables is specified). Output files are named <table>.csv.
    """
    import psycopg

    from r2g.connectors.postgres import PostgresConnector

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    try:
        pg_schema_name = schema_filter or "public"
        connector = PostgresConnector(connection_string, schema_name=pg_schema_name)
        schema = connector.get_schema()
        table_names = sorted(schema.tables.keys())

        if tables:
            requested = {t.strip() for t in tables.split(",")}
            missing = requested - set(table_names)
            if missing:
                console.print(f"[yellow]Warning: tables not found in schema: {', '.join(sorted(missing))}[/yellow]")
            table_names = [t for t in table_names if t in requested]

        if not table_names:
            console.print("[yellow]No tables to dump.[/yellow]")
            return

        console.print(f"[green]Dumping {len(table_names)} tables from PostgreSQL to {output_dir}/[/green]")

        with psycopg.connect(connection_string) as conn:
            for tbl in table_names:
                csv_path = out / f"{tbl}.csv"
                copy_sql = f"COPY {schema_filter}.{tbl} TO STDOUT WITH CSV HEADER"
                with csv_path.open("wb") as f:
                    with conn.cursor().copy(copy_sql) as copy:
                        for chunk in copy:
                            f.write(chunk)
                row_count = sum(1 for _ in csv_path.open("r")) - 1
                console.print(f"  [cyan]{tbl}[/cyan] → {csv_path} ({row_count} rows)")

        console.print(f"[green]Done. {len(table_names)} CSV files written to {output_dir}/[/green]")
    except Exception as e:
        log.exception("dump_tables_failed")
        console.print(f"[red]Failed to dump tables:[/red] {e}")
        raise typer.Exit(code=1)


# ── CDC commands ─────────────────────────────────────────────────────


@app.command("cdc-setup")
def cdc_setup(
    pg_conn: str = typer.Option(
        ..., "--pg-conn", help="PostgreSQL connection string", envvar="PG_CONN"
    ),
    slot_name: str = typer.Option(
        "r2g_slot", "--slot", help="Replication slot name"
    ),
    plugin: str = typer.Option(
        "test_decoding", "--plugin",
        help="Output plugin (test_decoding or wal2json)",
    ),
) -> None:
    """Create a logical replication slot for CDC."""
    from r2g.cdc.pg_listener import PGReplicationListener

    try:
        listener = PGReplicationListener(
            pg_conn_string=pg_conn,
            handler=None,  # type: ignore[arg-type]
            slot_name=slot_name,
            plugin=plugin,
        )
        status = listener.setup()
        console.print("[green]Replication slot ready:[/green]")
        for k, v in status.items():
            console.print(f"  {k}: {v}")
    except Exception as e:
        log.exception("cdc_setup_failed")
        console.print(f"[red]CDC setup failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("cdc-teardown")
def cdc_teardown(
    pg_conn: str = typer.Option(
        ..., "--pg-conn", help="PostgreSQL connection string", envvar="PG_CONN"
    ),
    slot_name: str = typer.Option(
        "r2g_slot", "--slot", help="Replication slot name"
    ),
    plugin: str = typer.Option(
        "test_decoding", "--plugin",
        help="Output plugin (test_decoding or wal2json)",
    ),
) -> None:
    """Drop a logical replication slot."""
    from r2g.cdc.pg_listener import PGReplicationListener

    try:
        listener = PGReplicationListener(
            pg_conn_string=pg_conn,
            handler=None,  # type: ignore[arg-type]
            slot_name=slot_name,
            plugin=plugin,
        )
        dropped = listener.drop_slot()
        if dropped:
            console.print(f"[green]Slot '{slot_name}' dropped.[/green]")
        else:
            console.print(f"[yellow]Slot '{slot_name}' not found.[/yellow]")
    except Exception as e:
        log.exception("cdc_teardown_failed")
        console.print(f"[red]CDC teardown failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("cdc-status")
def cdc_status(
    pg_conn: str = typer.Option(
        ..., "--pg-conn", help="PostgreSQL connection string", envvar="PG_CONN"
    ),
    slot_name: str = typer.Option(
        "r2g_slot", "--slot", help="Replication slot name"
    ),
    plugin: str = typer.Option(
        "test_decoding", "--plugin",
        help="Output plugin (test_decoding or wal2json)",
    ),
) -> None:
    """Show the status of a logical replication slot."""
    from r2g.cdc.pg_listener import PGReplicationListener

    try:
        listener = PGReplicationListener(
            pg_conn_string=pg_conn,
            handler=None,  # type: ignore[arg-type]
            slot_name=slot_name,
            plugin=plugin,
        )
        status = listener.slot_status()
        if status is None:
            console.print(f"[yellow]Slot '{slot_name}' not found.[/yellow]")
            raise typer.Exit(code=1)
        table = RichTable(title=f"Replication Slot: {slot_name}")
        table.add_column("Property")
        table.add_column("Value")
        for k, v in status.items():
            table.add_row(k, str(v))
        console.print(table)
    except typer.Exit:
        raise
    except Exception as e:
        log.exception("cdc_status_failed")
        console.print(f"[red]CDC status failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("cdc-start")
def cdc_start(
    pg_conn: str = typer.Option(
        ..., "--pg-conn", help="PostgreSQL connection string", envvar="PG_CONN"
    ),
    schema_file: str = typer.Argument(..., help="Path to schema JSON file"),
    config_path: str = typer.Argument(..., help="Path to mapping config YAML/JSON"),
    endpoint: str = typer.Option(
        "http://localhost:8529", "--endpoint",
        help="ArangoDB endpoint", envvar="ARANGO_ENDPOINT",
    ),
    database: str = typer.Option(
        "_system", "--database",
        help="ArangoDB database name", envvar="ARANGO_DB",
    ),
    username: str = typer.Option(
        "root", "--username",
        help="ArangoDB username", envvar="ARANGO_USER",
    ),
    password: str = typer.Option(
        "", "--password",
        help="ArangoDB password", envvar="ARANGO_PASSWORD",
    ),
    slot_name: str = typer.Option(
        "r2g_slot", "--slot", help="Replication slot name"
    ),
    plugin: str = typer.Option(
        "test_decoding", "--plugin",
        help="Output plugin (test_decoding or wal2json)",
    ),
    poll_interval: float = typer.Option(
        1.0, "--poll-interval",
        help="Seconds between polls when no changes are pending",
    ),
    batch_size: int = typer.Option(
        1000, "--batch-size",
        help="Maximum changes to consume per poll cycle",
    ),
    create_slot: bool = typer.Option(
        True, "--create-slot/--no-create-slot",
        help="Automatically create the replication slot if it doesn't exist",
    ),
    conflict_policy: str = typer.Option(
        "source_wins", "--conflict-policy",
        help="Conflict resolution policy: source_wins, last_write_wins, log_and_skip, fail",
    ),
    temporal: bool = typer.Option(
        False, "--temporal/--no-temporal",
        help="Use temporal (immutable-proxy) versioned writes instead of direct replace/delete",
    ),
    ttl_seconds: int = typer.Option(
        30 * 24 * 60 * 60, "--ttl-seconds",
        help="Retention (seconds) for historical versions before TTL GC (temporal mode)",
    ),
    smart_field: str = typer.Option(
        "", "--smart-field",
        help="Shard-key attribute for SmartGraph key prefixes (temporal mode, P5.8)",
    ),
) -> None:
    """Start the CDC listener (continuous polling for PostgreSQL changes).

    Connects to PostgreSQL via a logical replication slot and ArangoDB
    via the HTTP API.  Polls for row-level changes, transforms them
    through the mapping config, and applies deltas in near real-time.

    Conflict policies:
      source_wins     - PG is truth; upsert on duplicate, insert on missing (default)
      last_write_wins - compare LSN; reject stale writes
      log_and_skip    - log conflicts, skip writes
      fail            - raise on any conflict

    With ``--temporal`` each change is applied as a versioned write using the
    immutable-proxy pattern (ProxyIn/Entity/ProxyOut + hasVersion edges) so
    full history and point-in-time queries are preserved; deletes become soft
    deletes and historical versions age out after ``--ttl-seconds``.

    Press Ctrl+C to stop gracefully.
    """
    from r2g.cdc.conflict import ConflictPolicy
    from r2g.cdc.handler import CDCHandler
    from r2g.cdc.pg_listener import PGReplicationListener
    from r2g.connectors.arango_writer import ArangoWriter
    from r2g.temporal.models import TemporalConfig

    try:
        policy = ConflictPolicy(conflict_policy)
    except ValueError:
        console.print(
            f"[red]Invalid conflict policy:[/red] '{conflict_policy}'. "
            f"Choose from: source_wins, last_write_wins, log_and_skip, fail"
        )
        raise typer.Exit(code=1)

    temporal_config = (
        TemporalConfig(ttl_retain_seconds=ttl_seconds, smart_field=smart_field or None)
        if temporal else None
    )

    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)

        writer = ArangoWriter(
            endpoint=endpoint,
            database=database,
            username=username,
            password=password,
        )
        writer.ensure_database()
        writer.connect()

        handler = CDCHandler(
            writer, schema, mapping, conflict_policy=policy,
            temporal=temporal, temporal_config=temporal_config,
        )
        if temporal:
            console.print(
                f"[cyan]Temporal mode enabled[/cyan] "
                f"(versioned writes, ttl={ttl_seconds}s"
                + (f", smart_field={smart_field}" if smart_field else "") + ")"
            )
        listener = PGReplicationListener(
            pg_conn_string=pg_conn,
            handler=handler,
            slot_name=slot_name,
            plugin=plugin,
            poll_interval=poll_interval,
            batch_size=batch_size,
        )

        if create_slot:
            listener.setup()

        console.print(
            f"[green]CDC listener starting[/green] "
            f"(slot={slot_name}, plugin={plugin}, "
            f"poll={poll_interval}s, batch={batch_size})"
        )
        console.print("[dim]Press Ctrl+C to stop.[/dim]")

        listener.run()

        stats = handler.stats.as_dict()
        console.print("\n[green]CDC listener stopped.[/green]")
        stats_table = RichTable(title="CDC Session Statistics")
        stats_table.add_column("Metric")
        stats_table.add_column("Value", justify="right")
        for k, v in stats.items():
            stats_table.add_row(k, str(v))
        console.print(stats_table)

        conflict_summary = handler.resolver.log.summary()
        if conflict_summary["total_conflicts"] > 0:
            ct = RichTable(title=f"Conflicts (policy: {policy.value})")
            ct.add_column("Type")
            ct.add_column("Count", justify="right")
            for ctype, count in conflict_summary["by_type"].items():
                ct.add_row(ctype, str(count))
            ct.add_row("[bold]Total[/bold]", f"[bold]{conflict_summary['total_conflicts']}[/bold]")
            console.print(ct)

        writer.close()

    except Exception as e:
        log.exception("cdc_start_failed")
        console.print(f"[red]CDC start failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def kafka_start(
    schema_file: Path = typer.Argument(..., help="Path to schema JSON"),
    config_path: Path = typer.Argument(..., help="Path to mapping YAML"),
    brokers: str = typer.Option(
        "localhost:9092", "--brokers",
        help="Kafka bootstrap servers (comma-separated)",
    ),
    topics: str = typer.Option(
        ..., "--topics",
        help="Comma-separated list of Kafka topics to consume",
    ),
    group_id: str = typer.Option(
        "r2g-cdc", "--group-id",
        help="Kafka consumer group ID",
    ),
    message_format: str = typer.Option(
        "debezium", "--format",
        help="Message format: debezium, flat",
    ),
    auto_offset_reset: str = typer.Option(
        "earliest", "--offset-reset",
        help="Where to start consuming: earliest, latest",
    ),
    batch_size: int = typer.Option(
        500, "--batch-size",
        help="Max messages to consume per poll",
    ),
    endpoint: str = typer.Option(
        "http://localhost:8529", "--endpoint",
        help="ArangoDB endpoint URL",
    ),
    database: str = typer.Option(
        "_system", "--database",
        help="ArangoDB database name",
    ),
    username: str = typer.Option("root", "--username", help="ArangoDB username"),
    password: str = typer.Option("", "--password", help="ArangoDB password"),
    conflict_policy: str = typer.Option(
        "source_wins", "--conflict-policy",
        help="Conflict resolution: source_wins, last_write_wins, log_and_skip, fail",
    ),
    temporal: bool = typer.Option(
        False, "--temporal/--no-temporal",
        help="Use temporal (immutable-proxy) versioned writes instead of direct replace/delete",
    ),
    ttl_seconds: int = typer.Option(
        30 * 24 * 60 * 60, "--ttl-seconds",
        help="Retention (seconds) for historical versions before TTL GC (temporal mode)",
    ),
    smart_field: str = typer.Option(
        "", "--smart-field",
        help="Shard-key attribute for SmartGraph key prefixes (temporal mode, P5.8)",
    ),
) -> None:
    """Start the Kafka CDC consumer (Debezium or flat JSON messages).

    Connects to Kafka broker(s), consumes change events from the
    specified topics, transforms them through the mapping config,
    and applies deltas to ArangoDB.

    Requires confluent-kafka: pip install 'r2g-arango[kafka]'

    With ``--temporal`` each change is applied as a versioned write using the
    immutable-proxy pattern, preserving full history and point-in-time queries.

    Press Ctrl+C to stop gracefully.
    """
    from r2g.cdc.conflict import ConflictPolicy
    from r2g.cdc.handler import CDCHandler
    from r2g.connectors.arango_writer import ArangoWriter
    from r2g.temporal.models import TemporalConfig

    try:
        policy = ConflictPolicy(conflict_policy)
    except ValueError:
        console.print(
            f"[red]Invalid conflict policy:[/red] '{conflict_policy}'. "
            "Choose from: source_wins, last_write_wins, log_and_skip, fail"
        )
        raise typer.Exit(code=1)

    temporal_config = (
        TemporalConfig(ttl_retain_seconds=ttl_seconds, smart_field=smart_field or None)
        if temporal else None
    )

    try:
        from r2g.cdc.kafka_consumer import KafkaConsumer
    except ImportError:
        console.print(
            "[red]confluent-kafka is not installed.[/red] "
            "Install it with: [bold]pip install 'r2g-arango[kafka]'[/bold]"
        )
        raise typer.Exit(code=1)

    try:
        schema = Schema.load_from_file(schema_file)
        mapping = ConfigManager.load_config(config_path)

        writer = ArangoWriter(
            endpoint=endpoint,
            database=database,
            username=username,
            password=password,
        )
        writer.ensure_database()
        writer.connect()

        handler = CDCHandler(
            writer, schema, mapping, conflict_policy=policy,
            temporal=temporal, temporal_config=temporal_config,
        )
        if temporal:
            console.print(
                f"[cyan]Temporal mode enabled[/cyan] "
                f"(versioned writes, ttl={ttl_seconds}s"
                + (f", smart_field={smart_field}" if smart_field else "") + ")"
            )

        topic_list = [t.strip() for t in topics.split(",") if t.strip()]
        if not topic_list:
            console.print("[red]No topics specified.[/red]")
            raise typer.Exit(code=1)

        consumer = KafkaConsumer(
            handler=handler,
            brokers=brokers,
            topics=topic_list,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            message_format=message_format,
            batch_size=batch_size,
        )

        console.print(
            f"[green]Kafka consumer starting[/green] -- "
            f"brokers={brokers}, topics={topic_list}, "
            f"group={group_id}, format={message_format}"
        )
        consumer.run()

        stats = handler.stats.as_dict()
        stats_table = RichTable(title="Kafka CDC Session Statistics")
        stats_table.add_column("Metric")
        stats_table.add_column("Value", justify="right")
        for k, v in stats.items():
            stats_table.add_row(k, str(v))
        console.print(stats_table)

        conflict_summary = handler.resolver.log.summary()
        if conflict_summary["total_conflicts"] > 0:
            ct = RichTable(title=f"Conflicts (policy: {policy.value})")
            ct.add_column("Type")
            ct.add_column("Count", justify="right")
            for ctype, count in conflict_summary["by_type"].items():
                ct.add_row(ctype, str(count))
            ct.add_row("[bold]Total[/bold]", f"[bold]{conflict_summary['total_conflicts']}[/bold]")
            console.print(ct)

        writer.close()

    except Exception as e:
        log.exception("kafka_start_failed")
        console.print(f"[red]Kafka consumer failed:[/red] {e}")
        raise typer.Exit(code=1)


# ── Mapping diff / selective reload ──────────────────────────────────


@app.command("mapping-diff")
def mapping_diff_cmd(
    old_config: str = typer.Argument(..., help="Path to old mapping config YAML"),
    new_config: str = typer.Argument(..., help="Path to new mapping config YAML"),
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    json_output: bool = typer.Option(False, "--json", help="Output diff as JSON"),
) -> None:
    """Compare two mapping configs and show what ArangoDB changes are needed."""
    from r2g.mapping_diff import diff_mappings

    try:
        old = ConfigManager.load_config(old_config)
        new = ConfigManager.load_config(new_config)
        schema = Schema.load_from_file(schema_file)
        plan = diff_mappings(old, new, schema)

        if not plan.changes:
            console.print("[green]Mappings are identical[/green]")
            return

        if json_output:
            console.print(plan.model_dump_json(indent=2))
            return

        changes_table = RichTable(title="Mapping Changes")
        changes_table.add_column("Type", style="magenta")
        changes_table.add_column("Collection / Edge", style="cyan")
        changes_table.add_column("Details")
        for change in plan.changes:
            target = change.collection or change.edge or ""
            details = ", ".join(f"{k}={v}" for k, v in change.details.items()) if change.details else ""
            changes_table.add_row(change.change_type, target, details)
        console.print(changes_table)

        actions_table = RichTable(title="Reload Actions")
        actions_table.add_column("Action", style="magenta")
        actions_table.add_column("Collection", style="cyan")
        actions_table.add_column("Reason")
        for action in plan.actions:
            actions_table.add_row(action.action_type, action.collection, action.reason)
        console.print(actions_table)

    except Exception as e:
        log.exception("mapping_diff_failed")
        console.print(f"[red]Mapping diff failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("selective-reload")
def selective_reload_cmd(
    old_config: str = typer.Argument(..., help="Path to old mapping config YAML"),
    new_config: str = typer.Argument(..., help="Path to new mapping config YAML"),
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    pg_conn: Optional[str] = typer.Option(
        None, "--pg-conn", help="PostgreSQL connection string", envvar="PG_CONN"
    ),
    endpoint: str = typer.Option(
        "http://localhost:8529", "--endpoint", help="ArangoDB endpoint", envvar="ARANGO_ENDPOINT"
    ),
    database: str = typer.Option("_system", "--database", "-d", help="ArangoDB database", envvar="ARANGO_DB"),
    username: str = typer.Option("root", "--username", "-u", help="ArangoDB username", envvar="ARANGO_USER"),
    password: str = typer.Option("", "--password", "-p", help="ArangoDB password", envvar="ARANGO_PASSWORD"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show plan without executing"),
    batch_size: int = typer.Option(10000, "--batch-size", help="Rows per batch for reload"),
    on_duplicate: str = typer.Option("replace", "--on-duplicate", help="On duplicate strategy"),
) -> None:
    """Compute and execute a selective reload based on mapping changes."""
    from r2g.connectors.arango_writer import ArangoWriter
    from r2g.mapping_diff import diff_mappings
    from r2g.selective_reload import SelectiveReloader

    try:
        old = ConfigManager.load_config(old_config)
        new = ConfigManager.load_config(new_config)
        schema = Schema.load_from_file(schema_file)
        plan = diff_mappings(old, new, schema)

        if not plan.changes:
            console.print("[green]No changes detected[/green]")
            return

        changes_table = RichTable(title="Mapping Changes")
        changes_table.add_column("Type", style="magenta")
        changes_table.add_column("Collection / Edge", style="cyan")
        changes_table.add_column("Details")
        for change in plan.changes:
            target = change.collection or change.edge or ""
            details = ", ".join(f"{k}={v}" for k, v in change.details.items()) if change.details else ""
            changes_table.add_row(change.change_type, target, details)
        console.print(changes_table)

        writer = ArangoWriter(
            endpoint=endpoint,
            database=database,
            username=username,
            password=password,
        )

        reloader = SelectiveReloader(
            writer=writer,
            plan=plan,
            pg_conn_string=pg_conn,
            schema=schema,
            config=new,
            batch_size=batch_size,
            on_duplicate=on_duplicate,
        )
        report = reloader.execute(dry_run=dry_run)

        report_table = RichTable(title="Reload Report" + (" (dry run)" if dry_run else ""))
        report_table.add_column("Action", style="magenta")
        report_table.add_column("Collection", style="cyan")
        report_table.add_column("Status")
        report_table.add_column("Detail")
        for entry in report.actions_executed:
            report_table.add_row(
                entry.get("action", ""),
                entry.get("collection", ""),
                "[green]executed[/green]",
                entry.get("reason", ""),
            )
        for entry in report.actions_skipped:
            report_table.add_row(
                entry.get("action", ""),
                entry.get("collection", ""),
                "[yellow]skipped[/yellow]",
                entry.get("reason", ""),
            )
        for entry in report.errors:
            report_table.add_row(
                entry.get("action", ""),
                entry.get("collection", ""),
                "[red]error[/red]",
                entry.get("error", ""),
            )
        console.print(report_table)

        if report.rows_reloaded:
            console.print(f"[green]Rows reloaded:[/green] {report.rows_reloaded:,}")

    except Exception as e:
        log.exception("selective_reload_failed")
        console.print(f"[red]Selective reload failed:[/red] {e}")
        raise typer.Exit(code=1)


@app.command("ui")
def ui_cmd(
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address"),
    port: int = typer.Option(8501, "--port", help="Port to serve on"),
    project: Optional[str] = typer.Option(None, "--project", help="Open directly to a project"),
    catalog_dir: Optional[str] = typer.Option(None, "--catalog-dir", help="Catalog directory"),
) -> None:
    """Start the R2G Mapping Studio web UI."""
    try:
        import uvicorn

        from r2g.ui.server import create_app
    except ImportError:
        console.print(
            "[red]FastAPI/Uvicorn not installed.[/red] "
            "Install with: [bold]pip install 'r2g-arango[ui]'[/bold]"
        )
        raise typer.Exit(code=1)

    console.print(f"[green]R2G Mapping Studio[/green] starting at http://{host}:{port}")
    if project:
        console.print(f"  Default project: {project}")

    app_instance = create_app(catalog_dir=catalog_dir)
    uvicorn.run(app_instance, host=host, port=port, log_level="info")


@app.command("mcp")
def mcp_cmd(
    transport: str = typer.Option("stdio", "--transport", "-t", help="Transport: stdio or sse"),
    host: str = typer.Option("127.0.0.1", "--host", help="Bind address (SSE only)"),
    port: int = typer.Option(8502, "--port", help="Port (SSE only)"),
) -> None:
    """Start the R2G MCP server for AI agent integration.

    The MCP server exposes R2G capabilities (schema introspection, mapping
    generation, data loading, validation) as tools that AI agents can call.

    Use stdio transport for Cursor / Claude Desktop integration.
    Use SSE transport for remote or multi-client access.
    """
    try:
        from r2g.mcp_server import mcp as mcp_app
    except ImportError:
        console.print(
            "[red]MCP SDK not installed.[/red] Install with: [bold]pip install 'r2g-arango[mcp]'[/bold]"
        )
        raise typer.Exit(code=1)

    if transport == "sse":
        console.print(f"[green]R2G MCP Server[/green] (SSE) starting at http://{host}:{port}/sse")
        mcp_app.run(transport="sse", host=host, port=port)
    else:
        mcp_app.run(transport="stdio")


# ── Source commands ───────────────────────────────────────────────────


def _get_catalog():
    from r2g.catalog import CatalogManager
    return CatalogManager()


@source_app.command("add")
def source_add(
    name: str = typer.Option(..., "--name", help="Source name"),
    source_type: str = typer.Option("postgresql", "--type", help="Source type"),
    conn: str = typer.Option(..., "--conn", help="Connection string"),
    description: str = typer.Option("", "--description", help="Description"),
    owner: str = typer.Option("", "--owner", help="Owner"),
) -> None:
    """Register a new data source."""
    try:
        mgr = _get_catalog()
        source = mgr.add_source(name, source_type, conn, description=description, owner=owner)
        console.print(f"[green]Source '{source.name}' added.[/green]")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1)
    except Exception as e:
        log.exception("source_add_failed")
        console.print(f"[red]Failed to add source:[/red] {e}")
        raise typer.Exit(code=1)


@source_app.command("list")
def source_list() -> None:
    """List all registered data sources."""
    mgr = _get_catalog()
    sources = mgr.list_sources()
    if not sources:
        console.print("[dim]No sources registered.[/dim]")
        return
    table = RichTable(title="Data Sources")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Description")
    table.add_column("Owner")
    table.add_column("Updated")
    for s in sources:
        table.add_row(s.name, s.source_type, s.description, s.owner, s.updated_at.isoformat())
    console.print(table)


@source_app.command("remove")
def source_remove(
    name: str = typer.Argument(..., help="Source name to remove"),
) -> None:
    """Remove a registered data source."""
    mgr = _get_catalog()
    if mgr.remove_source(name):
        console.print(f"[green]Source '{name}' removed.[/green]")
    else:
        console.print(f"[yellow]Source '{name}' not found.[/yellow]")
        raise typer.Exit(code=1)


@source_app.command("snapshot")
def source_snapshot(
    name: str = typer.Argument(..., help="Source name to snapshot"),
    pg_schema: str = typer.Option(
        "public",
        "--pg-schema",
        help="Schema to introspect (PostgreSQL schema or Snowflake schema)",
    ),
    compare_last: bool = typer.Option(False, "--compare-last", help="Diff against previous snapshot"),
) -> None:
    """Introspect the schema from the source and save a snapshot.

    Uses the source's ``source_type`` (``postgresql`` or ``snowflake``)
    to pick the right connector.
    """
    from r2g.connectors.base import create_source_connector

    mgr = _get_catalog()
    source = mgr.get_source(name)
    if source is None:
        console.print(f"[red]Source '{name}' not found.[/red]")
        raise typer.Exit(code=1)

    try:
        connector = create_source_connector(
            source.source_type or "postgresql",
            source.connection_string,
            schema_name=pg_schema,
            source_params=source.source_params,
        )
        schema = connector.get_schema()
        previous = mgr.get_latest_snapshot(name) if compare_last else None
        snap = mgr.create_snapshot(name, schema, pg_schema=pg_schema)
        console.print(
            f"[green]Snapshot created:[/green] {snap.id}\n"
            f"  {len(schema.tables)} tables captured at {snap.captured_at.isoformat()}"
        )

        if compare_last and previous is not None:
            from r2g.schema_diff import diff_schemas

            diff = diff_schemas(previous.schema_data, schema)
            has_changes = diff["added_tables"] or diff["removed_tables"] or diff["modified_tables"]
            if not has_changes:
                console.print("[green]No schema changes since last snapshot.[/green]")
            else:
                if diff["added_tables"]:
                    console.print("[green]Added tables:[/green] " + ", ".join(diff["added_tables"]))
                if diff["removed_tables"]:
                    console.print("[red]Removed tables:[/red] " + ", ".join(diff["removed_tables"]))
                if diff["modified_tables"]:
                    console.print(f"[yellow]Modified tables:[/yellow] {', '.join(diff['modified_tables'].keys())}")
        elif compare_last and previous is None:
            console.print("[dim]No previous snapshot to compare against.[/dim]")
    except Exception as e:
        log.exception("source_snapshot_failed")
        console.print(f"[red]Snapshot failed:[/red] {e}")
        raise typer.Exit(code=1)


@source_app.command("dump")
def source_dump(
    name: str = typer.Argument(..., help="Catalog source name"),
    output_dir: str = typer.Option(
        "./dumps", "--output-dir", "-o", help="Directory to write CSV files"
    ),
    pg_schema: str = typer.Option(
        "public",
        "--pg-schema",
        help="Source schema to dump (PG/Snowflake schema name)",
    ),
    tables: Optional[str] = typer.Option(
        None,
        "--tables",
        "-t",
        help="Comma-separated list of tables (default: latest snapshot's tables)",
    ),
) -> None:
    """Dump every table in a cataloged source to CSV files.

    Source-agnostic replacement for the legacy ``r2g dump-tables
    --conn <pg_url>``. Uses :meth:`SourceSession.dump_table_to_csv`,
    so PostgreSQL goes through ``COPY TO STDOUT`` and Snowflake streams
    through the cursor; both produce comma-separated, header-row CSV.
    """
    from r2g.connectors.base import create_source_connector

    mgr = _get_catalog()
    source = mgr.get_source(name)
    if source is None:
        console.print(f"[red]Source '{name}' not found in catalog.[/red]")
        raise typer.Exit(code=1)

    snap = mgr.get_latest_snapshot(name)
    if tables:
        table_names = [t.strip() for t in tables.split(",") if t.strip()]
    elif snap is not None:
        table_names = sorted(snap.schema_data.tables.keys())
    else:
        console.print(
            "[red]No --tables given and no snapshot exists. "
            "Run `r2g source snapshot` first or pass --tables.[/red]"
        )
        raise typer.Exit(code=1)

    if not table_names:
        console.print("[yellow]No tables to dump.[/yellow]")
        return

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    try:
        connector = create_source_connector(
            source.source_type or "postgresql",
            source.connection_string,
            schema_name=pg_schema,
            source_params=source.source_params,
        )
    except ImportError as err:
        console.print(f"[red]{err}[/red]")
        raise typer.Exit(code=1)
    except ValueError as err:
        console.print(f"[red]{err}[/red]")
        raise typer.Exit(code=2)

    console.print(
        f"[green]Dumping {len(table_names)} tables from "
        f"{source.source_type} source '{name}' → {output_dir}/[/green]"
    )

    session = connector.open_session()
    try:
        total_rows = 0
        for tbl in table_names:
            csv_path = out / f"{tbl}.csv"
            try:
                rows = session.dump_table_to_csv(tbl, csv_path)
            except Exception as err:  # noqa: BLE001
                log.exception("source_dump_table_failed", source=name, table=tbl)
                console.print(f"  [red]{tbl}[/red] → failed: {err}")
                continue
            total_rows += rows
            console.print(f"  [cyan]{tbl}[/cyan] → {csv_path} ({rows} rows)")
        console.print(
            f"[green]Done. {len(table_names)} CSV files written "
            f"({total_rows:,} rows total).[/green]"
        )
    finally:
        session.close()


@source_app.command("infer-fks")
def source_infer_fks(
    name: str = typer.Argument(..., help="Source name"),
    sample: bool = typer.Option(
        False,
        "--sample",
        help="Run bounded value-overlap queries to score candidates (PostgreSQL only)",
    ),
    sample_limit: int = typer.Option(
        10_000,
        "--sample-limit",
        help="Row cap per side for --sample queries",
    ),
    min_confidence: float = typer.Option(
        0.4,
        "--min-confidence",
        help="Drop candidates below this confidence (0..1)",
    ),
    accept: bool = typer.Option(
        False,
        "--accept",
        help=(
            "Write accepted candidates back into the latest snapshot as "
            "declared foreign keys. Skips anything already declared."
        ),
    ),
) -> None:
    """Propose foreign keys for a source's latest schema snapshot.

    Uses the stored ``source_type`` (PostgreSQL or Snowflake) for the
    name-based heuristic. ``--sample`` additionally opens a PostgreSQL
    connection and runs small ``LEFT JOIN`` queries to score
    value-overlap between candidate columns; Snowflake sampling is
    not supported in this slice and falls back to name-only.
    """
    from rich.table import Table as RichTable

    from r2g.fk_inference import (
        InferenceOptions,
        PostgresValueSampler,
        infer_foreign_keys,
    )

    mgr = _get_catalog()
    source = mgr.get_source(name)
    if source is None:
        console.print(f"[red]Source '{name}' not found.[/red]")
        raise typer.Exit(code=1)
    snap = mgr.get_latest_snapshot(name)
    if snap is None:
        console.print(
            f"[red]No snapshot for '{name}'. Run `r2g source snapshot {name}` first.[/red]"
        )
        raise typer.Exit(code=1)

    sampler = None
    stype = (source.source_type or "postgresql").lower()
    if sample and stype in ("postgresql", "postgres", "pg"):
        sampler = PostgresValueSampler(
            source.connection_string,
            schema_name=snap.pg_schema,
            limit=sample_limit,
        )
    elif sample:
        console.print(
            f"[yellow]--sample is only supported for PostgreSQL sources (got "
            f"'{stype}'); falling back to name-only inference.[/yellow]"
        )

    opts = InferenceOptions(min_confidence=min_confidence, sample_overlap=bool(sampler))
    try:
        candidates = infer_foreign_keys(snap.schema_data, options=opts, sampler=sampler)
    finally:
        if sampler is not None:
            sampler.close()

    if not candidates:
        console.print("[dim]No FK candidates met the confidence threshold.[/dim]")
        return

    tbl = RichTable(title=f"Inferred FK candidates for '{name}'")
    tbl.add_column("Table")
    tbl.add_column("Columns")
    tbl.add_column("→ Foreign")
    tbl.add_column("Conf", justify="right")
    tbl.add_column("Method")
    for c in candidates:
        tbl.add_row(
            c.table,
            ", ".join(c.columns),
            f"{c.foreign_table}({', '.join(c.foreign_columns)})",
            f"{c.confidence:.2f}",
            c.method,
        )
    console.print(tbl)

    if accept:
        from r2g.types import ForeignKey

        accepted = 0
        schema = snap.schema_data
        for c in candidates:
            tbl_def = schema.tables.get(c.table)
            if tbl_def is None:
                continue
            existing = {tuple(sorted(fk.columns)) for fk in tbl_def.foreign_keys}
            if tuple(sorted(c.columns)) in existing:
                continue
            tbl_def.foreign_keys.append(
                ForeignKey(
                    columns=list(c.columns),
                    foreign_table=c.foreign_table,
                    foreign_columns=list(c.foreign_columns),
                    constraint_name=f"inferred_{c.method}",
                )
            )
            accepted += 1
        if accepted:
            mgr.create_snapshot(name, schema, pg_schema=snap.pg_schema)
            console.print(
                f"[green]Accepted {accepted} candidate(s); wrote a new snapshot "
                f"with merged FKs.[/green]"
            )
        else:
            console.print("[dim]No new FKs to accept.[/dim]")


# ── Project commands ─────────────────────────────────────────────────


@project_app.command("create")
def project_create(
    name: str = typer.Option(..., "--name", help="Project name"),
    source: str = typer.Option(..., "--source", help="Source name"),
    mapping: str = typer.Option(..., "--mapping", help="Path to mapping config"),
    endpoint: str = typer.Option("http://localhost:8529", "--endpoint", help="ArangoDB endpoint URL"),
    database: str = typer.Option("_system", "--database", help="ArangoDB database name"),
) -> None:
    """Create a new project."""
    try:
        mgr = _get_catalog()
        project = mgr.create_project(name, source, mapping, arango_endpoint=endpoint, arango_database=database)
        console.print(f"[green]Project '{project.name}' created.[/green]")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1)
    except Exception as e:
        log.exception("project_create_failed")
        console.print(f"[red]Failed to create project:[/red] {e}")
        raise typer.Exit(code=1)


@project_app.command("list")
def project_list() -> None:
    """List all projects."""
    mgr = _get_catalog()
    projects = mgr.list_projects()
    if not projects:
        console.print("[dim]No projects registered.[/dim]")
        return
    table = RichTable(title="Projects")
    table.add_column("Name", style="cyan")
    table.add_column("Source", style="magenta")
    table.add_column("Mapping Config")
    table.add_column("ArangoDB Endpoint")
    table.add_column("Database")
    for p in projects:
        table.add_row(p.name, p.source_name, p.mapping_config_path, p.arango_endpoint, p.arango_database)
    console.print(table)


@project_app.command("status")
def project_status(
    name: str = typer.Argument(..., help="Project name"),
) -> None:
    """Show the status of a project (last load, snapshot age, mapping path)."""
    mgr = _get_catalog()
    project = mgr.get_project(name)
    if project is None:
        console.print(f"[red]Project '{name}' not found.[/red]")
        raise typer.Exit(code=1)

    table = RichTable(title=f"Project: {name}")
    table.add_column("Property")
    table.add_column("Value")
    table.add_row("Source", project.source_name)
    table.add_row("Mapping Config", project.mapping_config_path)
    table.add_row("ArangoDB Endpoint", project.arango_endpoint)
    table.add_row("ArangoDB Database", project.arango_database)
    table.add_row("Schema Snapshot ID", project.schema_snapshot_id or "[dim]none[/dim]")

    history = mgr.get_history(project_name=name, limit=1)
    if history:
        last = history[0]
        table.add_row("Last Load Status", last.status)
        table.add_row("Last Load Type", last.load_type)
        table.add_row("Last Load Rows", str(last.rows_loaded))
        table.add_row("Last Load Started", last.started_at.isoformat())
    else:
        table.add_row("Last Load", "[dim]none[/dim]")

    console.print(table)


# ── History command ──────────────────────────────────────────────────


@app.command("history")
def history_cmd(
    project: Optional[str] = typer.Option(None, "--project", help="Filter by project name"),
    limit: int = typer.Option(20, "--limit", help="Max records to show"),
) -> None:
    """Show load history."""
    mgr = _get_catalog()
    records = mgr.get_history(project_name=project, limit=limit)
    if not records:
        console.print("[dim]No load history found.[/dim]")
        return
    table = RichTable(title="Load History")
    table.add_column("ID", style="dim", max_width=8)
    table.add_column("Project", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Status")
    table.add_column("Rows", justify="right")
    table.add_column("Errors", justify="right")
    table.add_column("Started")
    for r in records:
        status_style = {"completed": "green", "failed": "red", "running": "yellow"}.get(r.status, "")
        table.add_row(
            r.id[:8],
            r.project_name,
            r.load_type,
            f"[{status_style}]{r.status}[/{status_style}]" if status_style else r.status,
            str(r.rows_loaded),
            str(r.errors),
            r.started_at.isoformat(),
        )
    console.print(table)


secrets_app = typer.Typer(help="Manage the R2G catalog secret key.")
app.add_typer(secrets_app, name="secrets")


@secrets_app.command("init")
def secrets_init(
    force: bool = typer.Option(False, "--force", help="Overwrite any existing key file"),
) -> None:
    """Initialize (or replace) the on-disk catalog secret key.

    Respects ``R2G_SECRET_KEY`` when set: if the env var is present the
    on-disk key file is not touched and this command is a no-op.
    """
    import os
    from pathlib import Path

    from cryptography.fernet import Fernet

    from r2g.security import SECRET_ENV, SECRET_FILENAME

    if os.environ.get(SECRET_ENV):
        console.print(f"[yellow]{SECRET_ENV} is set; on-disk key is ignored while it is present.[/yellow]")
        return

    path = Path.home() / ".r2g" / SECRET_FILENAME
    if path.exists() and not force:
        console.print(f"[yellow]Secret key already exists at {path} (use --force to replace).[/yellow]")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(fd, Fernet.generate_key())
    finally:
        os.close(fd)
    os.chmod(path, 0o600)
    console.print(f"[green]New secret key written to {path} (0600).[/green]")
    console.print(
        "[dim]Tip: back up this file. Losing it makes the encrypted catalog values unrecoverable.[/dim]"
    )


@secrets_app.command("migrate")
def secrets_migrate() -> None:
    """Force-encrypt every secret in the catalog with the active key.

    Reads the catalog, re-writes it with the active key. Any already-encrypted
    values are left alone; plaintext values are encrypted in place. Useful
    after upgrading from a version that predates at-rest encryption.
    """
    mgr = _get_catalog()
    catalog = mgr._load()
    plaintext_sources = [s.name for s in catalog.sources.values() if s.connection_string]
    plaintext_targets = [t.name for t in catalog.targets.values() if t.password]
    mgr._save(catalog)
    console.print(
        f"[green]Re-encrypted {len(plaintext_sources)} sources and {len(plaintext_targets)} targets.[/green]"
    )


@secrets_app.command("status")
def secrets_status() -> None:
    """Show where the active secret key is coming from."""
    import os
    from pathlib import Path

    from r2g.security import SECRET_ENV, SECRET_FILENAME

    if os.environ.get(SECRET_ENV):
        console.print(f"[green]Using {SECRET_ENV} environment variable.[/green]")
        return
    path = Path.home() / ".r2g" / SECRET_FILENAME
    if path.exists():
        console.print(f"[green]Using key file {path}.[/green]")
    else:
        console.print(
            f"[yellow]No key file at {path}. It will be created the next time the catalog is opened.[/yellow]"
        )


if __name__ == "__main__":
    app()
