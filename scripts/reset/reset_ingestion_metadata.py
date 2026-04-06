from __future__ import annotations

from b2b_ec_pipeline.state import ETL_METADATA_SCHEMA, ensure_etl_metadata_schema
from b2b_ec_sources import get_connection
from rich.console import Console
from rich.table import Table

console = Console()


def reset_ingestion_metadata(stages: list[str]) -> None:
    stages_set = sorted(set(stages))
    deleted_counts = {
        "watermarks": 0,
        "run_manifests": 0,
        "checkpoints": 0,
        "schema_snapshots": 0,
    }

    ensure_etl_metadata_schema(ETL_METADATA_SCHEMA)

    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {ETL_METADATA_SCHEMA}.watermarks WHERE stage = ANY(%s)",
                (stages_set,),
            )
            deleted_counts["watermarks"] = cur.rowcount

            cur.execute(
                f"DELETE FROM {ETL_METADATA_SCHEMA}.run_manifests WHERE stage = ANY(%s)",
                (stages_set,),
            )
            deleted_counts["run_manifests"] = cur.rowcount

            cur.execute(
                f"DELETE FROM {ETL_METADATA_SCHEMA}.checkpoints WHERE stage = ANY(%s)",
                (stages_set,),
            )
            deleted_counts["checkpoints"] = cur.rowcount

            cur.execute(
                f"DELETE FROM {ETL_METADATA_SCHEMA}.schema_snapshots WHERE stage = ANY(%s)",
                (stages_set,),
            )
            deleted_counts["schema_snapshots"] = cur.rowcount

    table = Table(title="Ingestion Metadata Reset")
    table.add_column("Category", style="cyan")
    table.add_column("Deleted", style="green", justify="right")
    for key, value in deleted_counts.items():
        table.add_row(key, str(value))

    console.print(f"[bold]Reset stages:[/bold] {stages_set}")
    console.print(table)


if __name__ == "__main__":
    stages_to_reset = ["process", "load"]
    reset_ingestion_metadata(stages=stages_to_reset)
