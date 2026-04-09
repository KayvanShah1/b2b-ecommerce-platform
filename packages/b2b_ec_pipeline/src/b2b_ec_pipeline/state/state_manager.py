from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

from b2b_ec_sources import get_connection
from psycopg2 import sql
from psycopg2.extras import Json
from pydantic import TypeAdapter

from b2b_ec_pipeline.state.bootstrap import ensure_etl_metadata_schema
from b2b_ec_pipeline.state.common import ETL_METADATA_SCHEMA, state_ref, to_json_safe
from b2b_ec_pipeline.state.models import DatasetSchemaSnapshot, IngestionCheckpoint, RunManifest, SchemaColumnSnapshot, Watermark
from b2b_ec_pipeline.state.snapshot_manager import IngestionSnapshotManager

schema_column_list_adapter = TypeAdapter(list[SchemaColumnSnapshot])


class IngestionStateManager:
    def __init__(self, snapshots: IngestionSnapshotManager, schema: str = ETL_METADATA_SCHEMA):
        self.snapshots = snapshots
        self.schema = schema
        self.archive_writer = snapshots.archive_writer

    def get_watermark(self, source: str, dataset: str, stage: str) -> Watermark | None:
        ensure_etl_metadata_schema(self.schema)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT source, dataset, stage, mode, watermark_column, watermark_value,
                               last_file, last_line, processed_files_count, updated_at, run_id
                        FROM {schema}.watermarks
                        WHERE source = %s AND dataset = %s AND stage = %s
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (source, dataset, stage),
                )
                row = cur.fetchone()
        if not row:
            return None
        return Watermark(
            source=row[0],
            dataset=row[1],
            stage=row[2],
            mode=row[3],
            watermark_column=row[4],
            value=row[5],
            last_file=row[6],
            last_line=row[7],
            processed_files_count=row[8],
            updated_at=row[9],
            run_id=row[10],
        )

    def put_watermark(self, watermark: Watermark) -> str:
        ensure_etl_metadata_schema(self.schema)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.watermarks (
                            source, dataset, stage, mode, watermark_column, watermark_value,
                            last_file, last_line, processed_files_count, run_id, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source, dataset, stage)
                        DO UPDATE SET
                            mode = EXCLUDED.mode,
                            watermark_column = EXCLUDED.watermark_column,
                            watermark_value = EXCLUDED.watermark_value,
                            last_file = EXCLUDED.last_file,
                            last_line = EXCLUDED.last_line,
                            processed_files_count = EXCLUDED.processed_files_count,
                            run_id = EXCLUDED.run_id,
                            updated_at = EXCLUDED.updated_at
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (
                        str(watermark.source),
                        watermark.dataset,
                        str(watermark.stage),
                        watermark.mode,
                        watermark.watermark_column,
                        Json(to_json_safe(watermark.value)),
                        watermark.last_file,
                        watermark.last_line,
                        watermark.processed_files_count,
                        watermark.run_id,
                        watermark.updated_at,
                    ),
                )
        self.archive_writer.archive_watermark(watermark)
        return state_ref(self.schema, "watermarks", str(watermark.source), watermark.dataset, str(watermark.stage))

    def get_run_manifest(self, run_id: str, source: str, dataset: str, stage: str) -> RunManifest | None:
        ensure_etl_metadata_schema(self.schema)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        SELECT run_ts, status, record_count, bad_record_count, watermark_before, watermark_after,
                               started_at, finished_at, failed_at, error_message
                        FROM {schema}.runs
                        WHERE run_id = %s AND source = %s AND dataset = %s AND stage = %s
                        LIMIT 1
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (run_id, source, dataset, stage),
                )
                row = cur.fetchone()
                if not row:
                    return None

                cur.execute(
                    sql.SQL(
                        """
                        SELECT path_kind, path
                        FROM {schema}.run_paths
                        WHERE run_id = %s AND source = %s AND dataset = %s AND stage = %s
                        ORDER BY path_kind, path_index
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (run_id, source, dataset, stage),
                )
                path_rows = cur.fetchall()

        processed_files: list[str] = []
        raw_paths: list[str] = []
        processed_paths: list[str] = []
        for path_kind, path in path_rows:
            if path_kind == "processed_file":
                processed_files.append(path)
            elif path_kind == "raw_path":
                raw_paths.append(path)
            elif path_kind == "processed_path":
                processed_paths.append(path)

        return RunManifest(
            source=source,
            dataset=dataset,
            stage=stage,
            run_id=run_id,
            run_ts=row[0],
            status=row[1],
            record_count=row[2],
            bad_record_count=row[3],
            watermark_before=row[4] or {},
            watermark_after=row[5] or {},
            started_at=row[6],
            finished_at=row[7],
            failed_at=row[8],
            error_message=row[9],
            processed_files=processed_files,
            raw_paths=raw_paths,
            processed_paths=processed_paths,
        )

    def put_run_manifest(self, manifest: RunManifest) -> str:
        ensure_etl_metadata_schema(self.schema)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.runs (
                            run_id, source, dataset, stage, run_ts, status, record_count, bad_record_count,
                            watermark_before, watermark_after, started_at, finished_at, failed_at, error_message, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (run_id, source, dataset, stage)
                        DO UPDATE SET
                            run_ts = EXCLUDED.run_ts,
                            status = EXCLUDED.status,
                            record_count = EXCLUDED.record_count,
                            bad_record_count = EXCLUDED.bad_record_count,
                            watermark_before = EXCLUDED.watermark_before,
                            watermark_after = EXCLUDED.watermark_after,
                            started_at = EXCLUDED.started_at,
                            finished_at = EXCLUDED.finished_at,
                            failed_at = EXCLUDED.failed_at,
                            error_message = EXCLUDED.error_message,
                            updated_at = NOW()
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (
                        manifest.run_id,
                        str(manifest.source),
                        manifest.dataset,
                        str(manifest.stage),
                        manifest.run_ts,
                        str(manifest.status),
                        manifest.record_count,
                        manifest.bad_record_count,
                        Json(to_json_safe(manifest.watermark_before)),
                        Json(to_json_safe(manifest.watermark_after)),
                        manifest.started_at,
                        manifest.finished_at,
                        manifest.failed_at,
                        manifest.error_message,
                    ),
                )

                cur.execute(
                    sql.SQL(
                        """
                        DELETE FROM {schema}.run_paths
                        WHERE run_id = %s AND source = %s AND dataset = %s AND stage = %s
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (manifest.run_id, str(manifest.source), manifest.dataset, str(manifest.stage)),
                )

                path_rows: list[tuple[str, int, str]] = []
                path_rows.extend(("processed_file", idx, path) for idx, path in enumerate(manifest.processed_files))
                path_rows.extend(("raw_path", idx, path) for idx, path in enumerate(manifest.raw_paths))
                path_rows.extend(("processed_path", idx, path) for idx, path in enumerate(manifest.processed_paths))
                if path_rows:
                    cur.executemany(
                        sql.SQL(
                            """
                            INSERT INTO {schema}.run_paths (
                                run_id, source, dataset, stage, path_kind, path_index, path, updated_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                            """
                        ).format(schema=sql.Identifier(self.schema)),
                        [
                            (
                                manifest.run_id,
                                str(manifest.source),
                                manifest.dataset,
                                str(manifest.stage),
                                path_kind,
                                path_index,
                                path,
                            )
                            for path_kind, path_index, path in path_rows
                        ],
                    )

        self.archive_writer.archive_run(manifest)
        return state_ref(
            self.schema, "runs", manifest.run_id, str(manifest.source), manifest.dataset, str(manifest.stage)
        )

    def start_run(self, manifest: RunManifest) -> RunManifest:
        started = manifest.model_copy(
            update={"status": "started", "started_at": manifest.started_at or datetime.now(timezone.utc)}
        )
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
        manifest = self.state_manager.get_run_manifest(run_id=watermark.run_id, source=source, dataset=dataset, stage=stage)
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

    def complete(self, **kwargs: Any) -> RunManifest:
        return self.state_manager.complete_run(self._updated_manifest(**kwargs))

    def fail(self, error_message: str, **kwargs: Any) -> RunManifest:
        return self.state_manager.fail_run(self._updated_manifest(**kwargs), error_message=error_message)

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
    failure_context: Callable[[], dict[str, Any]] | None = None,
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
        fail_kwargs = failure_context() if failure_context else {}
        fail_kwargs.setdefault("watermark_before", run_ctx.watermark_before)
        run_ctx.fail(error_message=str(exc), **fail_kwargs)
        run_ctx.checkpoint("status", "failed")
        raise
