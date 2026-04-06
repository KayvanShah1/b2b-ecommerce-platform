import uuid
from datetime import datetime
from typing import Any

import polars as pl
from b2b_ec_utils import settings
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.storage import storage
from b2b_ec_utils.timer import timed_run

from b2b_ec_pipeline.ingestion.models import (
    FileLoadResult,
    FILE_LOAD_SPECS,
    LoadDatasetSpec,
    LoadTargetSpec,
    PostgresLoadResult,
    PostgresTableConfig,
)
from b2b_ec_pipeline.state import RunManifest, managed_ingestion_run, state_manager

logger = get_logger("StagingLoad")


def _quote(identifier: str) -> str:
    return f'"{identifier}"'


def _read_parquet(path: str) -> pl.DataFrame:
    with storage.open(path, mode="rb") as file_handle:
        return pl.read_parquet(file_handle)


def _resolve_paths(hinted_paths: list[str] | None) -> list[str]:
    return sorted(set(hinted_paths or []))


def _schema_columns(dataframe: pl.DataFrame) -> list[dict[str, Any]]:
    return [{"name": name, "dtype": str(dtype), "nullable": True} for name, dtype in dataframe.schema.items()]


def _upsert_dataframe(
    conn,
    dataframe: pl.DataFrame,
    target: LoadTargetSpec,
    *,
    replace_full_snapshot: bool = True,
) -> None:
    if dataframe.is_empty():
        return

    schema_ref = _quote(settings.ingestion.load_schema)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")
    temp_table = f"tmp_{target.table}_{uuid.uuid4().hex[:8]}"
    conn.register(temp_table, dataframe.to_arrow())

    target_ref = f"{schema_ref}.{_quote(target.table)}"
    temp_ref = _quote(temp_table)

    if target.full_snapshot:
        if replace_full_snapshot:
            conn.execute(f"CREATE OR REPLACE TABLE {target_ref} AS SELECT * FROM {temp_ref}")
        else:
            conn.execute(f"INSERT INTO {target_ref} SELECT * FROM {temp_ref}")
        conn.unregister(temp_table)
        return

    conn.execute(f"CREATE TABLE IF NOT EXISTS {target_ref} AS SELECT * FROM {temp_ref} WHERE 1=0")

    if target.primary_key:
        delete_join = " AND ".join([f"t.{_quote(column)} = s.{_quote(column)}" for column in target.primary_key])
        conn.execute(
            f"""
            DELETE FROM {target_ref} AS t
            USING {temp_ref} AS s
            WHERE {delete_join}
            """
        )

    conn.execute(f"INSERT INTO {target_ref} SELECT * FROM {temp_ref}")
    conn.unregister(temp_table)


def _load_dataset(
    conn,
    spec: LoadDatasetSpec,
    run_id: str,
    run_ts: datetime,
    hinted_paths: list[str] | None = None,
) -> RunManifest:
    processed_paths = _resolve_paths(hinted_paths)
    logger.info(
        f"STAGING INPUT: source={spec.source} dataset={spec.dataset} input_files={len(processed_paths)} "
        "mode=manifest_only"
    )

    loaded_rows = 0
    schema_columns: list[dict[str, Any]] = []
    with managed_ingestion_run(
        state_manager=state_manager,
        run_id=run_id,
        source=spec.source,
        dataset=spec.dataset,
        run_ts=run_ts,
        stage="load",
        processed_files=hinted_paths or [],
        failure_context=lambda: {
            "processed_paths": processed_paths,
            "record_count": loaded_rows,
        },
    ) as run_ctx:
        previous_value = run_ctx.watermark_before.get("value")
        full_snapshot_initialized: set[str] = set()
        for processed_path in processed_paths:
            dataframe = _read_parquet(processed_path)
            if dataframe.is_empty():
                run_ctx.checkpoint("last_file", processed_path)
                continue

            for target in spec.targets:
                replace_full_snapshot = target.table not in full_snapshot_initialized
                logger.info(
                    f"STAGING UPSERT START: source={spec.source} dataset={spec.dataset} "
                    f"target={target.table} file={processed_path} rows={dataframe.height}"
                )
                _upsert_dataframe(
                    conn,
                    dataframe,
                    target,
                    replace_full_snapshot=replace_full_snapshot,
                )
                if target.full_snapshot:
                    full_snapshot_initialized.add(target.table)
                logger.info(
                    f"STAGING UPSERT COMPLETE: source={spec.source} dataset={spec.dataset} "
                    f"target={target.table} file={processed_path}"
                )

            loaded_rows += dataframe.height
            if not schema_columns:
                schema_columns = _schema_columns(dataframe)

            run_ctx.checkpoint("last_file", processed_path)
            logger.info(
                f"STAGING FILE: source={spec.source} dataset={spec.dataset} file={processed_path} rows={dataframe.height}"
            )

        watermark_after = {
            "source": spec.source,
            "dataset": spec.dataset,
            "stage": "load",
            "mode": spec.mode,
            "value": processed_paths[-1] if processed_paths else previous_value,
            "updated_at": run_ts,
            "run_id": run_id,
        }
        completed_manifest = run_ctx.complete(
            record_count=loaded_rows,
            processed_paths=processed_paths,
            watermark_before=run_ctx.watermark_before,
            watermark_after=watermark_after,
        )
        if schema_columns:
            run_ctx.snapshot(columns=schema_columns, row_count=loaded_rows, captured_at=run_ts)
        run_ctx.checkpoint("status", "completed")
        logger.info(
            f"STAGING LOAD: source={spec.source} dataset={spec.dataset} files={len(processed_paths)} rows={loaded_rows}"
        )
        return completed_manifest


def _failed_manifest(spec: LoadDatasetSpec, run_id: str, run_ts: datetime, error_message: str) -> RunManifest:
    return RunManifest(
        source=spec.source,
        dataset=spec.dataset,
        stage="load",
        run_id=run_id,
        run_ts=run_ts,
        status="failed",
        record_count=0,
        bad_record_count=0,
        processed_paths=[],
        error_message=error_message,
    )


def _postgres_load_spec(table_cfg: PostgresTableConfig) -> LoadDatasetSpec:
    return LoadDatasetSpec(
        source="postgres",
        dataset=table_cfg.name,
        mode=table_cfg.mode,
        targets=[
            LoadTargetSpec(
                table=table_cfg.name,
                primary_key=table_cfg.primary_key,
                full_snapshot=(table_cfg.mode == "full_snapshot"),
            )
        ],
    )


@timed_run
def load_postgres_manifests_to_staging(
    conn,
    table_configs: tuple[PostgresTableConfig, ...],
    run_id: str,
    run_ts: datetime,
    manifests: list[RunManifest] | None = None,
) -> PostgresLoadResult:
    hinted_paths_by_dataset = {manifest.dataset: manifest.processed_paths for manifest in manifests or []}
    load_manifests: list[RunManifest] = []
    loaded_rows = 0
    loaded_tables: list[str] = []

    for table_cfg in table_configs:
        spec = _postgres_load_spec(table_cfg)
        try:
            manifest = _load_dataset(
                conn,
                spec=spec,
                run_id=run_id,
                run_ts=run_ts,
                hinted_paths=hinted_paths_by_dataset.get(table_cfg.name),
            )
            load_manifests.append(manifest)
            loaded_rows += manifest.record_count
            loaded_tables.extend([target.table for target in spec.targets])
        except Exception as exc:
            logger.exception(f"STAGING LOAD FAILED: source=postgres dataset={table_cfg.name} run_id={run_id}: {exc}")
            load_manifests.append(_failed_manifest(spec, run_id=run_id, run_ts=run_ts, error_message=str(exc)))

    return PostgresLoadResult(manifests=load_manifests, loaded_rows=loaded_rows, loaded_tables=loaded_tables)


@timed_run
def load_file_manifests_to_staging(
    conn,
    run_id: str,
    run_ts: datetime,
    file_manifests: dict[str, RunManifest] | None = None,
) -> FileLoadResult:
    file_manifests = file_manifests or {}
    hints = {dataset_key: manifest.processed_paths for dataset_key, manifest in file_manifests.items()}
    manifests: dict[str, RunManifest] = {}
    loaded_rows = 0
    loaded_tables: list[str] = []

    for dataset_key, spec in FILE_LOAD_SPECS.items():
        hinted_paths = hints.get(dataset_key, [])
        try:
            manifest = _load_dataset(
                conn,
                spec=spec,
                run_id=run_id,
                run_ts=run_ts,
                hinted_paths=hinted_paths,
            )
            manifests[dataset_key] = manifest
            loaded_rows += manifest.record_count
            loaded_tables.extend([target.table for target in spec.targets])
        except Exception as exc:
            logger.exception(
                f"STAGING LOAD FAILED: source={spec.source} dataset={spec.dataset} run_id={run_id}: {exc}"
            )
            manifests[dataset_key] = _failed_manifest(spec, run_id=run_id, run_ts=run_ts, error_message=str(exc))

    return FileLoadResult(manifests=manifests, loaded_rows=loaded_rows, loaded_tables=loaded_tables)
