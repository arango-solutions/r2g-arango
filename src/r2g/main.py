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
) -> None:
    setup_logging(level="DEBUG" if verbose else "INFO", json_output=json_log)


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
    open_browser: bool = typer.Option(True, "--open/--no-open", help="Open in default browser"),
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
        if open_browser:
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
        transformers = {
            e: EdgeTransformer(e, table_def, key_separator=mapping.key_separator)
            for e in matching
        }

        if len(matching) == 1:
            out_paths = {matching[0]: out_path}
        else:
            base = out_path.stem
            parent = out_path.parent
            suffix = out_path.suffix if out_path.suffix else ".jsonl"
            out_paths = {
                e: parent / f"{base}_{e.edge_collection}{suffix}" for e in matching
            }

        handles: dict[EdgeDefinition, Any] = {}
        try:
            for e, path in out_paths.items():
                path.parent.mkdir(parents=True, exist_ok=True)
                handles[e] = path.open("w", encoding="utf-8")

            counts: dict[str, int] = {e.edge_collection: 0 for e in matching}
            row_num = 0
            for row in reader.read_rows():
                for edge_def in matching:
                    doc = transformers[edge_def].transform_row(row)
                    if doc is not None:
                        handles[edge_def].write(json.dumps(doc) + "\n")
                        counts[edge_def.edge_collection] += 1
                row_num += 1
                if limit is not None and row_num >= limit:
                    break
        finally:
            for h in handles.values():
                h.close()

        for e, path in out_paths.items():
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
    endpoint: str = typer.Option("http://127.0.0.1:8529", "--endpoint", help="ArangoDB endpoint URL"),
    database: str = typer.Option("_system", "--database", "-d", help="Database name"),
    username: str = typer.Option("root", "--username", "-u", help="ArangoDB username"),
    password: str = typer.Option("", "--password", "-p", help="ArangoDB password"),
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
    endpoint: str = typer.Option("http://127.0.0.1:8529", "--endpoint", help="ArangoDB endpoint URL"),
    database: str = typer.Option("_system", "--database", "-d", help="Database name"),
    username: str = typer.Option("root", "--username", "-u", help="ArangoDB username"),
    password: str = typer.Option("", "--password", "-p", help="ArangoDB password"),
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
    connection_string: str = typer.Option(..., "--conn", "-c", help="PostgreSQL connection string"),
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
    pg_conn: str = typer.Option(..., "--pg-conn", help="PostgreSQL connection string"),
    schema_file: str = typer.Option(..., "--schema", "-s", help="Path to schema.json"),
    config_path: str = typer.Option(..., "--config", "-c", help="Mapping config YAML"),
    endpoint: str = typer.Option("http://127.0.0.1:8529", "--endpoint", help="ArangoDB endpoint URL"),
    database: str = typer.Option("_system", "--database", "-d", help="ArangoDB database name"),
    username: str = typer.Option("root", "--username", "-u", help="ArangoDB username"),
    password: str = typer.Option("", "--password", "-p", help="ArangoDB password"),
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


@app.command("dump-tables")
def dump_tables(
    connection_string: str = typer.Option(..., "--conn", "-c", help="PostgreSQL connection string"),
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
