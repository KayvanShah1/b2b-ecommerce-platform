from __future__ import annotations

from rich.console import Console
from rich.panel import Panel

from scripts.reset.reset_minio import reset_minio_buckets
from scripts.reset.reset_motherduck import reset_motherduck_schemas
from scripts.reset.reset_postgres import reset_postgres_database

console = Console()


def reset_all(motherduck_schemas_to_delete, buckets_to_empty) -> None:
    console.print(
        Panel.fit(
            "[bold red]This will reset Postgres objects, DuckDB/MotherDuck objects, and empty data buckets.[/bold red]\n"
            "[yellow]Databases and buckets themselves are NOT deleted.[/yellow]",
            title="Reset Confirmation",
        )
    )
    confirm = console.input("[bold red]Proceed? (y/n): [/bold red]").strip().lower()
    if confirm != "y":
        console.print("[yellow]Reset cancelled.[/yellow]")
        return

    reset_postgres_database()
    reset_motherduck_schemas(motherduck_schemas_to_delete)
    reset_minio_buckets(buckets=buckets_to_empty)
    console.print("[bold green]All reset steps completed.[/bold green]")


if __name__ == "__main__":
    motherduck_schemas_to_delete = [
        "staging",
        "analytics",
    ]
    buckets_to_empty = [
        "webserver-logs",
        "marketing-leads",
        "raw-data",
        "metadata",
        "processed-data",
    ]
    reset_all(motherduck_schemas_to_delete, buckets_to_empty)
