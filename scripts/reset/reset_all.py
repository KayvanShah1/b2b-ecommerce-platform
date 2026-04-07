from __future__ import annotations

import sys

from b2b_ec_utils import get_logger, settings, timed_run
from rich.console import Console
from rich.panel import Panel

sys.path.append(str(settings.project_root / "scripts"))

from reset.reset_minio import reset_minio_buckets
from reset.reset_motherduck import reset_motherduck_schemas
from reset.reset_ingestion_metadata import reset_ingestion_metadata
from reset.reset_postgres import reset_postgres_database

console = Console()
logger = get_logger("ResetAll")


@timed_run
def reset_all(motherduck_schemas_to_delete, buckets_to_empty) -> None:
    logger.info("Starting full reset process...")
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
    reset_ingestion_metadata(stages=["raw_capture", "process", "load"], recreate_schema=True)
    reset_minio_buckets(buckets=buckets_to_empty)
    reset_motherduck_schemas(motherduck_schemas_to_delete)
    console.print("[bold green]All reset steps completed.[/bold green]")
    logger.info("Full reset process completed.")


if __name__ == "__main__":
    motherduck_schemas_to_delete = [
        "staging",
        "marts",
        "ingestion",
    ]
    buckets_to_empty = [
        settings.storage.webserver_logs_bucket,
        settings.storage.marketing_leads_bucket,
        settings.storage.raw_data_bucket,
        settings.storage.metadata_bucket,
        settings.storage.processed_data_bucket,
    ]
    reset_all(motherduck_schemas_to_delete, buckets_to_empty)
