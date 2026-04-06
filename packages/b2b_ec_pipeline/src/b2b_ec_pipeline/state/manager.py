from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
from threading import Lock
from typing import Any, Iterator

from psycopg2 import sql
from psycopg2.extras import Json
from pydantic import BaseModel, TypeAdapter

from b2b_ec_sources import get_connection

from b2b_ec_pipeline.state.models import (
    DatasetSchemaSnapshot,
    IngestionCheckpoint,
    RunManifest,
    SchemaColumnSnapshot,
    Watermark,
)

DEFAULT_STAGE_KEY = "__default__"
ETL_METADATA_SCHEMA = "etl_metadata"

_bootstrap_lock = Lock()
_bootstrapped_schemas: set[str] = set()
schema_column_list_adapter = TypeAdapter(list[SchemaColumnSnapshot])


def _stage_key(stage: str | None) -> str:
    return stage or DEFAULT_STAGE_KEY


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


def _state_ref(schema: str, table: str, *parts: str) -> str:
    suffix = "/".join(str(part) for part in parts)
    return f"postgres://{schema}/{table}/{suffix}" if suffix else f"postgres://{schema}/{table}"


def _ensure_schema_bootstrapped(schema: str) -> None:
    with _bootstrap_lock:
        if schema in _bootstrapped_schemas:
            return

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema}").format(schema=sql.Identifier(schema)))

                cur.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {schema}.watermarks (
                            source TEXT NOT NULL,
                            dataset TEXT NOT NULL,
                            stage_key TEXT NOT NULL,
                            stage TEXT,
                            payload JSONB NOT NULL,
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            PRIMARY KEY (source, dataset, stage_key)
                        )
                        """
                    ).format(schema=sql.Identifier(schema))
                )

                cur.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {schema}.run_manifests (
                            run_id TEXT NOT NULL,
                            source TEXT NOT NULL,
                            dataset TEXT NOT NULL,
                            stage_key TEXT NOT NULL,
                            stage TEXT,
                            status TEXT NOT NULL,
                            run_ts TIMESTAMPTZ,
                            payload JSONB NOT NULL,
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            PRIMARY KEY (run_id, source, dataset, stage_key)
                        )
                        """
                    ).format(schema=sql.Identifier(schema))
                )

                cur.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {schema}.checkpoints (
                            run_id TEXT NOT NULL,
                            source TEXT NOT NULL,
                            dataset TEXT NOT NULL,
                            stage_key TEXT NOT NULL,
                            stage TEXT,
                            checkpoint_name TEXT NOT NULL,
                            checkpoint_value JSONB,
                            payload JSONB NOT NULL,
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            PRIMARY KEY (run_id, source, dataset, stage_key, checkpoint_name)
                        )
                        """
                    ).format(schema=sql.Identifier(schema))
                )

                cur.execute(
                    sql.SQL(
                        """
                        CREATE TABLE IF NOT EXISTS {schema}.schema_snapshots (
                            run_id TEXT NOT NULL,
                            source TEXT NOT NULL,
                            dataset TEXT NOT NULL,
                            stage_key TEXT NOT NULL,
                            stage TEXT,
                            captured_at TIMESTAMPTZ NOT NULL,
                            payload JSONB NOT NULL,
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            PRIMARY KEY (run_id, source, dataset, stage_key)
                        )
                        """
                    ).format(schema=sql.Identifier(schema))
                )

                cur.execute(
                    sql.SQL(
                        """
                        CREATE INDEX IF NOT EXISTS idx_run_manifests_source_dataset_stage_updated
                        ON {schema}.run_manifests (source, dataset, stage, updated_at DESC)
                        """
                    ).format(schema=sql.Identifier(schema))
                )
                cur.execute(
                    sql.SQL(
                        """
                        CREATE INDEX IF NOT EXISTS idx_run_manifests_run_id
                        ON {schema}.run_manifests (run_id)
                        """
                    ).format(schema=sql.Identifier(schema))
                )
                cur.execute(
                    sql.SQL(
                        """
                        CREATE INDEX IF NOT EXISTS idx_checkpoints_source_dataset_stage_updated
                        ON {schema}.checkpoints (source, dataset, stage, updated_at DESC)
                        """
                    ).format(schema=sql.Identifier(schema))
                )

        _bootstrapped_schemas.add(schema)


def ensure_etl_metadata_schema(schema: str = ETL_METADATA_SCHEMA) -> None:
    _ensure_schema_bootstrapped(schema)


class IngestionSnapshotManager:
    def __init__(self, schema: str = ETL_METADATA_SCHEMA):
        self.schema = schema

    def put_checkpoint(self, checkpoint: IngestionCheckpoint) -> str:
        _ensure_schema_bootstrapped(self.schema)
        stage = str(checkpoint.stage)
        payload = checkpoint.model_dump(mode="json")

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.checkpoints (
                            run_id,
                            source,
                            dataset,
                            stage_key,
                            stage,
                            checkpoint_name,
                            checkpoint_value,
                            payload,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (run_id, source, dataset, stage_key, checkpoint_name)
                        DO UPDATE SET
                            checkpoint_value = EXCLUDED.checkpoint_value,
                            payload = EXCLUDED.payload,
                            updated_at = NOW()
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (
                        checkpoint.run_id,
                        str(checkpoint.source),
                        checkpoint.dataset,
                        _stage_key(stage),
                        stage,
                        checkpoint.checkpoint_name,
                        Json(_to_json_safe(checkpoint.checkpoint_value)),
                        Json(_to_json_safe(payload)),
                    ),
                )

        return _state_ref(
            self.schema,
            "checkpoints",
            checkpoint.run_id,
            str(checkpoint.source),
            checkpoint.dataset,
            stage,
            checkpoint.checkpoint_name,
        )

    def put_dataset_schema_snapshot(self, snapshot: DatasetSchemaSnapshot) -> str:
        _ensure_schema_bootstrapped(self.schema)
        stage = str(snapshot.stage)
        payload = snapshot.model_dump(mode="json")

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.schema_snapshots (
                            run_id,
                            source,
                            dataset,
                            stage_key,
                            stage,
                            captured_at,
                            payload,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (run_id, source, dataset, stage_key)
                        DO UPDATE SET
                            captured_at = EXCLUDED.captured_at,
                            payload = EXCLUDED.payload,
                            updated_at = NOW()
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (
                        snapshot.run_id,
                        str(snapshot.source),
                        snapshot.dataset,
                        _stage_key(stage),
                        stage,
                        snapshot.captured_at,
                        Json(_to_json_safe(payload)),
                    ),
                )

        return _state_ref(
            self.schema,
            "schema_snapshots",
            snapshot.run_id,
            str(snapshot.source),
            snapshot.dataset,
            stage,
        )


class IngestionStateManager:
    def __init__(self, snapshots: IngestionSnapshotManager, schema: str = ETL_METADATA_SCHEMA):
        self.snapshots = snapshots
        self.schema = schema

    def get_watermark(self, source: str, dataset: str, stage: str | None = None) -> Watermark | None:
        _ensure_schema_bootstrapped(self.schema)
        requested_stage = stage
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT payload
                        FROM {schema}.watermarks
                        WHERE source = %s AND dataset = %s AND stage_key = %s
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (source, dataset, _stage_key(requested_stage)),
                )
                row = cur.fetchone()

                if row is None and requested_stage is not None:
                    cur.execute(
                        sql.SQL(
                            """
                            SELECT payload
                            FROM {schema}.watermarks
                            WHERE source = %s AND dataset = %s AND stage_key = %s
                            """
                        ).format(schema=sql.Identifier(self.schema)),
                        (source, dataset, _stage_key(None)),
                    )
                    row = cur.fetchone()

        return Watermark.model_validate(row[0]) if row else None

    def put_watermark(self, watermark: Watermark) -> str:
        _ensure_schema_bootstrapped(self.schema)
        stage = str(watermark.stage) if watermark.stage else None
        payload = watermark.model_dump(mode="json")

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.watermarks (
                            source,
                            dataset,
                            stage_key,
                            stage,
                            payload,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (source, dataset, stage_key)
                        DO UPDATE SET
                            stage = EXCLUDED.stage,
                            payload = EXCLUDED.payload,
                            updated_at = NOW()
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (
                        str(watermark.source),
                        watermark.dataset,
                        _stage_key(stage),
                        stage,
                        Json(_to_json_safe(payload)),
                    ),
                )

        return _state_ref(self.schema, "watermarks", str(watermark.source), watermark.dataset, _stage_key(stage))

    def get_run_manifest(
        self,
        run_id: str,
        source: str,
        dataset: str,
        stage: str | None = None,
    ) -> RunManifest | None:
        _ensure_schema_bootstrapped(self.schema)
        with get_connection() as conn:
            with conn.cursor() as cur:
                if stage is None:
                    cur.execute(
                        sql.SQL(
                            """
                            SELECT payload
                            FROM {schema}.run_manifests
                            WHERE run_id = %s AND source = %s AND dataset = %s
                            ORDER BY updated_at DESC
                            LIMIT 1
                            """
                        ).format(schema=sql.Identifier(self.schema)),
                        (run_id, source, dataset),
                    )
                else:
                    cur.execute(
                        sql.SQL(
                            """
                            SELECT payload
                            FROM {schema}.run_manifests
                            WHERE run_id = %s AND source = %s AND dataset = %s AND stage_key = %s
                            LIMIT 1
                            """
                        ).format(schema=sql.Identifier(self.schema)),
                        (run_id, source, dataset, _stage_key(stage)),
                    )
                row = cur.fetchone()

        return RunManifest.model_validate(row[0]) if row else None

    def put_run_manifest(self, manifest: RunManifest) -> str:
        _ensure_schema_bootstrapped(self.schema)
        stage = str(manifest.stage) if manifest.stage else None
        payload = manifest.model_dump(mode="json")

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.run_manifests (
                            run_id,
                            source,
                            dataset,
                            stage_key,
                            stage,
                            status,
                            run_ts,
                            payload,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (run_id, source, dataset, stage_key)
                        DO UPDATE SET
                            stage = EXCLUDED.stage,
                            status = EXCLUDED.status,
                            run_ts = EXCLUDED.run_ts,
                            payload = EXCLUDED.payload,
                            updated_at = NOW()
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (
                        manifest.run_id,
                        str(manifest.source),
                        manifest.dataset,
                        _stage_key(stage),
                        stage,
                        str(manifest.status),
                        manifest.run_ts,
                        Json(_to_json_safe(payload)),
                    ),
                )

        return _state_ref(
            self.schema,
            "run_manifests",
            manifest.run_id,
            str(manifest.source),
            manifest.dataset,
            _stage_key(stage),
        )

    def start_run(self, manifest: RunManifest) -> RunManifest:
        started = manifest.model_copy(update={"status": "started", "started_at": manifest.started_at or datetime.now(timezone.utc)})
        self.put_run_manifest(started)
        return started

    def complete_run(self, manifest: RunManifest) -> RunManifest:
        completed = manifest.model_copy(
            update={"status": "completed", "finished_at": manifest.finished_at or datetime.now(timezone.utc)}
        )
        self.put_run_manifest(completed)
        return completed

    def fail_run(self, manifest: RunManifest, error_message: str) -> RunManifest:
        failed = manifest.model_copy(
            update={"status": "failed", "failed_at": datetime.now(timezone.utc), "error_message": error_message}
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


class StateResolver:
    def __init__(self, state_manager: IngestionStateManager):
        self.state_manager = state_manager

    def latest_completed_manifest(self, source: str, dataset: str, stage: str) -> RunManifest | None:
        watermark = self.state_manager.get_watermark(source=source, dataset=dataset, stage=stage)
        if not watermark or not watermark.run_id:
            return None

        manifest = self.state_manager.get_run_manifest(
            run_id=watermark.run_id,
            source=source,
            dataset=dataset,
            stage=stage,
        )
        if not manifest:
            return None
        if str(manifest.stage) != stage or manifest.status != "completed":
            return None
        return manifest


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
                updated_at=datetime.now(timezone.utc),
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
                captured_at=captured_at or datetime.now(timezone.utc),
                columns=normalized_columns,
                row_count=row_count,
            )
        )


@contextmanager
def managed_ingestion_run(
    *,
    state_manager: IngestionStateManager,
    run_id: str,
    source: str,
    dataset: str,
    run_ts: datetime,
    stage: str,
    processed_files: list[str] | None = None,
    auto_complete: bool = False,
) -> Iterator[IngestionRunContext]:
    run_ctx = state_manager.open_run(
        run_id=run_id,
        source=source,
        dataset=dataset,
        run_ts=run_ts,
        stage=stage,
        processed_files=processed_files,
    )
    try:
        yield run_ctx
        if auto_complete:
            run_ctx.complete(watermark_before=run_ctx.watermark_before)
            run_ctx.checkpoint("status", "completed")
    except Exception as exc:
        run_ctx.fail(error_message=str(exc), watermark_before=run_ctx.watermark_before)
        run_ctx.checkpoint("status", "failed")
        raise


snapshot_manager = IngestionSnapshotManager()
state_manager = IngestionStateManager(snapshot_manager)
state_resolver = StateResolver(state_manager)
