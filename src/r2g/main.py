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
    graph_name: Optional[str] = typer.Option(None, "--graph-name", help="Create a named graph after import via Gharial API"),
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
    config_path: Optional[str] = typer.Option(None, "--config", "-c", help="Mapping config YAML (enables type coercion)"),
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
) -> None:
    """Connect to PostgreSQL and extract schema metadata."""
    console.print("[green]Connecting to PostgreSQL...[/green]")

    try:
        connector = PostgresConnector(connection_string)
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
) -> None:
    """Stream data directly from PostgreSQL to ArangoDB (no intermediate files).

    Opens a PostgreSQL connection with REPEATABLE READ isolation for consistent
    snapshots, reads tables via server-side cursors in configurable batches,
    transforms rows on the fly, and bulk-imports into ArangoDB via the HTTP API.
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

        pipeline = StreamingPipeline(
            pg_conn_string=pg_conn,
            arango_writer=writer,
            schema=schema,
            config=mapping,
            batch_size=batch_size,
            on_duplicate=on_duplicate,
        )

        console.print(
            f"[green]Streaming from PostgreSQL → ArangoDB[/green]\n"
            f"  PG: {pg_conn.split('@')[-1] if '@' in pg_conn else pg_conn}\n"
            f"  ArangoDB: {endpoint}/{database}\n"
            f"  Batch size: {batch_size:,}"
        )

        results = pipeline.run(graph_name=graph_name)

        table = RichTable(title="Streaming Import Summary")
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
        console.print(
            f"[green]Stream complete:[/green] {total_docs:,} documents, "
            f"{total_edges:,} edges imported."
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
        connector = PostgresConnector(connection_string)
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
