from __future__ import annotations

import json
from typing import Iterable

from b2b_ec_utils.storage import storage
from rich.console import Console
from rich.table import Table

console = Console()


def _remove_path(path: str) -> bool:
    if not storage.exists(path):
        return False
    storage.fs.rm(path, recursive=True)
    return True


def _iter_run_manifest_paths() -> Iterable[str]:
    pattern = storage.get_metadata_path("runs", "*", "*", "*.json")
    return sorted(set(storage.glob(pattern)))


def reset_ingestion_metadata(stages: list[str]) -> None:
    stages_set = set(stages)
    deleted_counts = {
        "watermarks": 0,
        "lineage_checkpoints": 0,
        "lineage_snapshots": 0,
        "run_manifests": 0,
    }

    # 1) stage-scoped watermarks
    for source in ["postgres", "marketing_leads", "webserver_logs"]:
        for stage in stages_set:
            watermark_stage_path = storage.get_metadata_path("watermarks", source, stage)
            if _remove_path(watermark_stage_path):
                deleted_counts["watermarks"] += 1

    # 2) stage-scoped lineage
    for stage in stages_set:
        checkpoints_pattern = storage.get_metadata_path("lineage", "checkpoints", "*", "*", stage, "*", "*.json")
        snapshots_pattern = storage.get_metadata_path("lineage", "snapshots", "*", "*", stage, "*.json")

        for path in sorted(set(storage.glob(checkpoints_pattern))):
            storage.fs.rm(path)
            deleted_counts["lineage_checkpoints"] += 1

        for path in sorted(set(storage.glob(snapshots_pattern))):
            storage.fs.rm(path)
            deleted_counts["lineage_snapshots"] += 1

    # 3) run manifests (filter by manifest.stage)
    for manifest_path in _iter_run_manifest_paths():
        with storage.open(manifest_path, mode="rb") as file_handle:
            payload = json.loads(file_handle.read().decode("utf-8"))
        if payload.get("stage") in stages_set:
            storage.fs.rm(manifest_path)
            deleted_counts["run_manifests"] += 1

    table = Table(title="Ingestion Metadata Reset")
    table.add_column("Category", style="cyan")
    table.add_column("Deleted", style="green", justify="right")
    for key, value in deleted_counts.items():
        table.add_row(key, str(value))

    console.print(f"[bold]Reset stages:[/bold] {sorted(stages_set)}")
    console.print(table)


if __name__ == "__main__":
    # Example: reset process + load metadata to replay from existing raw/processed data.
    stages_to_reset = ["process", "load"]
    reset_ingestion_metadata(stages=stages_to_reset)
