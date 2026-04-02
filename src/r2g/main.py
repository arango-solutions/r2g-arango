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
from r2g.generators.arangoimport import ArangoImportGenerator
from r2g.input.dump_reader import DumpReader
from r2g.log import get_logger, setup_logging
from r2g.transformers.edge_transformer import EdgeTransformer
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import EdgeDefinition, Schema

app = typer.Typer(help="Antigravity R2G-ETL: Relational to Graph Pipeline")
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


if __name__ == "__main__":
    app()
