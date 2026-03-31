import json
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, TypeAdapter

from b2b_ec_utils.storage import storage

from b2b_ec_etl.defs.ingestion.core.models import (
    DatasetSchemaSnapshot,
    IngestionCheckpoint,
    RunManifest,
    SchemaColumnSnapshot,
    Watermark,
)


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _to_json_safe(inner_value) for key, inner_value in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(inner_value) for inner_value in value]
    return value


def _read_json(path: str) -> dict[str, Any] | None:
    if not storage.exists(path):
        return None
    with storage.open(path, mode="rb") as file_handle:
        payload = file_handle.read()
    if not payload:
        return None
    return json.loads(payload.decode("utf-8"))


def _write_json(path: str, payload: dict[str, Any]) -> None:
    with storage.open(path, mode="wb") as file_handle:
        file_handle.write(json.dumps(_to_json_safe(payload), indent=2, sort_keys=True).encode("utf-8"))


def _watermark_path(source: str, dataset: str, stage: str | None) -> str:
    parts = [source]
    if stage:
        parts.append(stage)
    parts.append(f"{dataset}.json")
    return storage.get_metadata_path("watermarks", *parts)


def _manifest_path(run_id: str, source: str, dataset: str) -> str:
    return storage.get_metadata_path("runs", run_id, source, f"{dataset}.json")


def _snapshot_path(run_id: str, source: str, dataset: str, stage: str) -> str:
    return storage.get_metadata_path("lineage", "snapshots", source, dataset, stage, f"{run_id}.json")


def _checkpoint_path(run_id: str, source: str, dataset: str, stage: str, checkpoint_name: str) -> str:
    return storage.get_metadata_path("lineage", "checkpoints", source, dataset, stage, run_id, f"{checkpoint_name}.json")


class IngestionSnapshotManager:
    def put_checkpoint(self, checkpoint: IngestionCheckpoint) -> str:
        path = _checkpoint_path(
            checkpoint.run_id,
            str(checkpoint.source),
            checkpoint.dataset,
            str(checkpoint.stage),
            checkpoint.checkpoint_name,
        )
        _write_json(path, checkpoint.model_dump(mode="json"))
        return path

    def put_dataset_schema_snapshot(self, snapshot: DatasetSchemaSnapshot) -> str:
        path = _snapshot_path(
            snapshot.run_id,
            str(snapshot.source),
            snapshot.dataset,
            str(snapshot.stage),
        )
        _write_json(path, snapshot.model_dump(mode="json"))
        return path


class IngestionStateManager:
    def __init__(self, snapshots: IngestionSnapshotManager):
        self.snapshots = snapshots

    def get_watermark(self, source: str, dataset: str, stage: str | None = None) -> Watermark | None:
        payload = _read_json(_watermark_path(source, dataset, stage))
        if payload is None and stage:
            payload = _read_json(_watermark_path(source, dataset, None))
        return Watermark.model_validate(payload) if payload else None

    def put_watermark(self, watermark: Watermark) -> str:
        path = _watermark_path(str(watermark.source), watermark.dataset, str(watermark.stage) if watermark.stage else None)
        _write_json(path, watermark.model_dump(mode="json"))
        return path

    def get_run_manifest(self, run_id: str, source: str, dataset: str) -> RunManifest | None:
        payload = _read_json(_manifest_path(run_id, source, dataset))
        return RunManifest.model_validate(payload) if payload else None

    def put_run_manifest(self, manifest: RunManifest) -> str:
        path = _manifest_path(manifest.run_id, str(manifest.source), manifest.dataset)
        _write_json(path, manifest.model_dump(mode="json"))
        return path

    def start_run(self, manifest: RunManifest) -> RunManifest:
        started = manifest.model_copy(update={"status": "started", "started_at": manifest.started_at or datetime.now()})
        self.put_run_manifest(started)
        return started

    def complete_run(self, manifest: RunManifest) -> RunManifest:
        completed = manifest.model_copy(update={"status": "completed", "finished_at": manifest.finished_at or datetime.now()})
        self.put_run_manifest(completed)
        return completed

    def fail_run(self, manifest: RunManifest, error_message: str) -> RunManifest:
        failed = manifest.model_copy(
            update={"status": "failed", "failed_at": datetime.now(), "error_message": error_message}
        )
        self.put_run_manifest(failed)
        return failed

    def open_run(
        self,
        run_id: str,
        source: str,
        dataset: str,
        run_ts: datetime,
        stage: str,
        processed_files: list[str] | None = None,
    ) -> "IngestionRunContext":
        return IngestionRunContext(
            state_manager=self,
            snapshot_manager=self.snapshots,
            run_id=run_id,
            source=source,
            dataset=dataset,
            stage=stage,
            run_ts=run_ts,
            processed_files=processed_files or [],
        )


class IngestionRunContext:
    def __init__(
        self,
        state_manager: IngestionStateManager,
        snapshot_manager: IngestionSnapshotManager,
        run_id: str,
        source: str,
        dataset: str,
        stage: str,
        run_ts: datetime,
        processed_files: list[str] | None = None,
    ):
        self.state_manager = state_manager
        self.snapshot_manager = snapshot_manager
        self.run_id = run_id
        self.source = source
        self.dataset = dataset
        self.stage = stage
        self.run_ts = run_ts

        self.watermark_before_model = self.state_manager.get_watermark(source, dataset, stage=stage)
        self.watermark_before = self.watermark_before_model.model_dump(mode="json") if self.watermark_before_model else {}

        self.manifest = self.state_manager.start_run(
            RunManifest(
                source=source,
                dataset=dataset,
                stage=stage,
                run_id=run_id,
                run_ts=run_ts,
                status="started",
                processed_files=processed_files or [],
                watermark_before=self.watermark_before,
            )
        )

    def _updated_manifest(self, **kwargs: Any) -> RunManifest:
        return self.manifest.model_copy(update=kwargs)

    def complete(self, **kwargs: Any) -> dict[str, Any]:
        completed = self.state_manager.complete_run(self._updated_manifest(**kwargs))
        return completed.model_dump(mode="json")

    def fail(self, error_message: str, **kwargs: Any) -> dict[str, Any]:
        failed = self.state_manager.fail_run(self._updated_manifest(**kwargs), error_message=error_message)
        return failed.model_dump(mode="json")

    def checkpoint(self, checkpoint_name: str, checkpoint_value: str | int | float | datetime | None) -> str:
        return self.snapshot_manager.put_checkpoint(
            IngestionCheckpoint(
                source=self.source,
                dataset=self.dataset,
                stage=self.stage,
                run_id=self.run_id,
                checkpoint_name=checkpoint_name,
                checkpoint_value=checkpoint_value,
                updated_at=datetime.now(),
            )
        )

    def snapshot(
        self,
        columns: list[dict[str, Any]] | list[SchemaColumnSnapshot],
        row_count: int | None = None,
        captured_at: datetime | None = None,
    ) -> str:
        normalized_columns = schema_column_list_adapter.validate_python(columns)
        return self.snapshot_manager.put_dataset_schema_snapshot(
            DatasetSchemaSnapshot(
                source=self.source,
                dataset=self.dataset,
                stage=self.stage,
                run_id=self.run_id,
                captured_at=captured_at or datetime.now(),
                columns=normalized_columns,
                row_count=row_count,
            )
        )


snapshot_manager = IngestionSnapshotManager()
state_manager = IngestionStateManager(snapshot_manager)
schema_column_list_adapter = TypeAdapter(list[SchemaColumnSnapshot])
