from __future__ import annotations

from b2b_ec_pipeline.state import (
    ETL_METADATA_SCHEMA,
    clear_etl_metadata_for_stages,
    ensure_etl_metadata_schema,
    recreate_etl_metadata_schema,
)
from rich.console import Console
from rich.table import Table

console = Console()


def reset_ingestion_metadata(stages: list[str], recreate_schema: bool = False) -> None:
    stages_set = sorted(set(stages))

    if recreate_schema:
        recreate_etl_metadata_schema(ETL_METADATA_SCHEMA)
    else:
        ensure_etl_metadata_schema(ETL_METADATA_SCHEMA)
    deleted_counts = clear_etl_metadata_for_stages(stages_set, schema=ETL_METADATA_SCHEMA)

    table = Table(title="Ingestion Metadata Reset")
    table.add_column("Category", style="cyan")
    table.add_column("Deleted", style="green", justify="right")
    if recreate_schema:
        table.add_row("schema_recreated", "1")
    for key, value in deleted_counts.items():
        table.add_row(key, str(value))

    console.print(f"[bold]Reset stages:[/bold] {stages_set}")
    console.print(table)


if __name__ == "__main__":
    stages_to_reset = ["process", "load"]
    reset_ingestion_metadata(stages=stages_to_reset)
