import json
from pathlib import Path
from typing import Any, Optional

import typer
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn
from rich.syntax import Syntax
from rich.table import Table as RichTable

from r2g.config import ConfigManager
from r2g.connectors.postgres import PostgresConnector
from r2g.generators.arangoimport import ArangoImportGenerator, CsvImportGenerator
from r2g.generators.visualizer import MappingVisualizer
from r2g.input.dump_reader import DumpReader
from r2g.log import get_logger, setup_logging
from r2g.transformers.edge_transformer import EdgeTransformer
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import EdgeDefinition, Schema

app = typer.Typer(help="R2G-ETL: Relational to Graph Pipeline")
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
    pg_conn: str = typer.Option(..., "--pg-conn", help="PostgreSQL connection string", envvar="PG_CONN"),
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
) -> None:
    """Stream data directly from PostgreSQL to ArangoDB (no intermediate files).

    Opens a PostgreSQL connection with REPEATABLE READ isolation for consistent
    snapshots, reads tables via server-side cursors in configurable batches,
    transforms rows on the fly, and bulk-imports into ArangoDB via the HTTP API.

    Use --dry-run to preview row counts and sample documents without writing.
    """
    from r2g.connectors.arango_writer import ArangoWriter
    from r2g.streaming.pipeline import StreamingPipeline

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

        pipeline = StreamingPipeline(
            pg_conn_string=pg_conn,
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
        )

        mode_label = "[yellow]DRY RUN[/yellow] — " if dry_run else ""
        console.print(
            f"{mode_label}[green]Streaming from PostgreSQL → ArangoDB[/green]\n"
            f"  PG: {pg_conn.split('@')[-1] if '@' in pg_conn else pg_conn}\n"
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


if __name__ == "__main__":
    app()
