from __future__ import annotations

from pathlib import Path

import duckdb
from rich.console import Console
from rich.table import Table

from b2b_ec_utils import settings

console = Console()


def _resolve_duckdb_database_uri() -> str:
    token = settings.motherduck.token.get_secret_value() if settings.motherduck.token else None
    if token and token != "<API_TOKEN>":
        return f"md:{settings.motherduck.database}?motherduck_token={token}"

    local_name = settings.motherduck.local_database
    local_path = Path(local_name)
    if not local_path.is_absolute():
        local_path = settings.local_warehouse_dir / local_name
    local_path.parent.mkdir(parents=True, exist_ok=True)
    return str(local_path)


def reset_motherduck_schemas(schema_names: list[str]) -> None:
    if not schema_names:
        console.print("[yellow]No schema names provided. Nothing to do.[/yellow]")
        return

    database = _resolve_duckdb_database_uri()
    console.print(f"[bold]Resetting selected DuckDB/MotherDuck schemas in:[/bold] [cyan]{database}[/cyan]")

    with duckdb.connect(database=database, read_only=False) as con:
        existing_schemas = {
            row[0] for row in con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
        }

        dropped = 0
        skipped = 0
        status_rows: list[tuple[str, str]] = []
        for schema_name in schema_names:
            if schema_name not in existing_schemas:
                skipped += 1
                status_rows.append((schema_name, "skipped (not found)"))
                continue

            con.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
            dropped += 1
            status_rows.append((schema_name, "dropped"))

    table = Table(title="DuckDB/MotherDuck Reset Summary")
    table.add_column("Schema", style="cyan")
    table.add_column("Status", style="green")
    for schema_name, status in status_rows:
        table.add_row(schema_name, status)
    table.add_row("[bold]Dropped total[/bold]", str(dropped))
    table.add_row("[bold]Skipped total[/bold]", str(skipped))
    console.print(table)
    console.print("[bold green]Done.[/bold green] Only selected schemas were processed.")


if __name__ == "__main__":
    schemas_to_delete = [
        "staging",
        "analytics",
    ]
    reset_motherduck_schemas(schemas_to_delete)
