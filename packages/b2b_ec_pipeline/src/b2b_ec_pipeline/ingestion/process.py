from datetime import datetime
from typing import Any, Callable

import polars as pl
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.storage import storage
from b2b_ec_utils.timer import timed_run

from b2b_ec_pipeline.ingestion.io import write_parquet_chunks
from b2b_ec_pipeline.ingestion.models import (
    FILE_PROCESS_SPECS,
    FILE_SOURCE_SCHEMAS,
    POSTGRES_TABLE_SCHEMAS,
    IngestionModel,
    ProcessDatasetSpec,
    PostgresTableConfig,
)
from b2b_ec_pipeline.state import managed_ingestion_run, state_manager

logger = get_logger("ProcessIngestion")
PROCESS_WRITE_CHUNK_SIZE = 50_000
FILE_PROCESS_DATASET_KEYS: tuple[str, ...] = tuple(FILE_PROCESS_SPECS.keys())


def _read_parquet(path: str) -> pl.DataFrame:
    with storage.open(path, mode="rb") as file_handle:
        return pl.read_parquet(file_handle)


def _required_columns(model_cls: type[IngestionModel]) -> list[str]:
    return [name for name, field in model_cls.model_fields.items() if field.is_required()]


def _ensure_model_columns(dataframe: pl.DataFrame, model_cls: type[IngestionModel]) -> pl.DataFrame:
    model_columns = list(model_cls.model_fields.keys())
    passthrough_columns = [column for column in dataframe.columns if column.startswith("_") and column not in model_columns]
    target_columns = model_columns + passthrough_columns
    missing_columns = [column for column in model_columns if column not in dataframe.columns]
    if missing_columns:
        dataframe = dataframe.with_columns([pl.lit(None).alias(column) for column in missing_columns])
    return dataframe.select(target_columns)


def _filter_required_non_null(dataframe: pl.DataFrame, required_cols: list[str]) -> tuple[pl.DataFrame, int]:
    if dataframe.is_empty() or not required_cols:
        return dataframe, 0

    valid_mask = pl.all_horizontal([pl.col(column).is_not_null() for column in required_cols])
    valid_df = dataframe.filter(valid_mask)
    bad_rows = dataframe.height - valid_df.height
    return valid_df, bad_rows


def _dedupe_frame(dataframe: pl.DataFrame, primary_key: tuple[str, ...], sort_column: str | None) -> pl.DataFrame:
    if dataframe.is_empty() or not primary_key:
        return dataframe
    dedupe_subset = [column for column in primary_key if column in dataframe.columns]
    if not dedupe_subset:
        return dataframe
    if sort_column and sort_column in dataframe.columns:
        dataframe = dataframe.sort(sort_column)
    return dataframe.unique(subset=dedupe_subset, keep="last")


def _schema_snapshot_columns(dataframe: pl.DataFrame) -> list[dict[str, Any]]:
    return [{"name": name, "dtype": str(dtype), "nullable": True} for name, dtype in dataframe.schema.items()]


def _parse_datetime_expression(column: str) -> pl.Expr:
    text_col = (
        pl.col(column)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace("T", " ", literal=True)
        .str.replace(r"(Z|[+-]\d{2}:\d{2})$", "", literal=False)
    )
    # Accept both generator formats after normalization:
    # - 2026-03-31 11:57:44.123456
    # - 2026-03-31 10:51:07
    return pl.coalesce(
        [
            text_col.str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f", strict=False, exact=False),
            text_col.str.to_datetime(format="%Y-%m-%d %H:%M:%S", strict=False, exact=False),
        ]
    )


def _write_processed_chunks(
    source: str,
    dataset: str,
    run_id: str,
    run_ts: datetime,
    dataframe: pl.DataFrame,
    file_index: int,
) -> tuple[list[str], int]:
    return write_parquet_chunks(
        dataframe=dataframe,
        chunk_size=PROCESS_WRITE_CHUNK_SIZE,
        output_path_for_chunk=lambda chunk_index: storage.get_processed_dataset_path(
            source,
            f"dataset={dataset}",
            f"run_date={run_ts.strftime('%Y-%m-%d')}",
            f"run_hour={run_ts.strftime('%H')}",
            f"{run_id}_file-{file_index:05d}-part-{chunk_index:05d}.parquet",
        ),
    )


def _preprocess_marketing(dataframe: pl.DataFrame) -> pl.DataFrame:
    expressions: list[pl.Expr] = []
    if "lead_id" in dataframe.columns:
        expressions.append(pl.col("lead_id").cast(pl.Utf8, strict=False))
    if "created_at" in dataframe.columns:
        expressions.append(_parse_datetime_expression("created_at"))
    if "status_updated_at" in dataframe.columns:
        expressions.append(_parse_datetime_expression("status_updated_at"))
    if "last_activity_at" in dataframe.columns:
        expressions.append(_parse_datetime_expression("last_activity_at"))
    if "estimated_annual_revenue" in dataframe.columns:
        expressions.append(pl.col("estimated_annual_revenue").cast(pl.Float64, strict=False))
    if "country_code" in dataframe.columns:
        expressions.append(pl.col("country_code").cast(pl.Utf8, strict=False).str.to_uppercase())
    if "is_prospect" in dataframe.columns:
        expressions.append(pl.col("is_prospect").cast(pl.Boolean, strict=False))
    if "_ingested_at" in dataframe.columns:
        expressions.append(_parse_datetime_expression("_ingested_at"))
    return dataframe.with_columns(expressions) if expressions else dataframe


def _preprocess_web_logs(dataframe: pl.DataFrame) -> pl.DataFrame:
    expressions: list[pl.Expr] = []
    if "event_id" in dataframe.columns:
        expressions.append(pl.col("event_id").cast(pl.Utf8, strict=False))
    if "event_ts" in dataframe.columns:
        expressions.append(_parse_datetime_expression("event_ts"))
    if "status_code" in dataframe.columns:
        expressions.append(pl.col("status_code").cast(pl.Int64, strict=False))
    if "_source_line" in dataframe.columns:
        expressions.append(pl.col("_source_line").cast(pl.Int64, strict=False))
    if "_ingested_at" in dataframe.columns:
        expressions.append(_parse_datetime_expression("_ingested_at"))
    return dataframe.with_columns(expressions) if expressions else dataframe


PREPROCESSORS: dict[str, Callable[[pl.DataFrame], pl.DataFrame]] = {
    "marketing_leads": _preprocess_marketing,
    "webserver_logs": _preprocess_web_logs,
}


def _resolve_candidate_paths(hint_raw_paths: list[str] | None) -> list[str]:
    return sorted(set(hint_raw_paths or []))


def _process_dataset(
    *,
    spec: ProcessDatasetSpec,
    run_id: str,
    run_ts: datetime,
    hint_raw_paths: list[str] | None = None,
) -> dict[str, Any]:
    source = spec.source
    dataset = spec.dataset
    model_cls = POSTGRES_TABLE_SCHEMAS.get(spec.model_key) or FILE_SOURCE_SCHEMAS[spec.model_key]
    preprocess = PREPROCESSORS.get(spec.preprocess or "")

    raw_paths: list[str] = []
    processed_paths: list[str] = []
    total_rows = 0
    bad_rows = 0
    schema_columns: list[dict[str, Any]] = []
    with managed_ingestion_run(
        state_manager=state_manager,
        run_id=run_id,
        source=source,
        dataset=dataset,
        run_ts=run_ts,
        stage="process",
        processed_files=hint_raw_paths or [],
        failure_context=lambda: {
            "raw_paths": raw_paths,
            "processed_paths": processed_paths,
            "record_count": total_rows,
            "bad_record_count": bad_rows,
        },
    ) as run_ctx:
        required_cols = _required_columns(model_cls)
        raw_paths = _resolve_candidate_paths(hint_raw_paths)
        logger.info(
            f"PROCESS INPUT: source={source} dataset={dataset} input_files={len(raw_paths)} "
            f"mode=manifest_only"
        )

        for file_index, raw_path in enumerate(raw_paths):
            dataframe = _read_parquet(raw_path)
            if preprocess:
                dataframe = preprocess(dataframe)
            dataframe = _ensure_model_columns(dataframe, model_cls)
            dataframe, file_bad_rows = _filter_required_non_null(dataframe, required_cols)
            bad_rows += file_bad_rows

            if file_bad_rows:
                logger.warning(
                    f"PROCESS VALIDATION: source={source} dataset={dataset} raw_path={raw_path} "
                    f"bad_rows={file_bad_rows}"
                )

            if dataframe.is_empty():
                run_ctx.checkpoint("last_file", raw_path)
                continue

            before_dedupe = dataframe.height
            dataframe = _dedupe_frame(dataframe, spec.dedupe_keys, spec.dedupe_sort_column)
            if dataframe.height != before_dedupe:
                logger.info(
                    f"PROCESS DEDUPE: source={source} dataset={dataset} raw_path={raw_path} "
                    f"before={before_dedupe} after={dataframe.height}"
                )

            if not schema_columns:
                schema_columns = _schema_snapshot_columns(dataframe)

            chunk_paths, chunk_rows = _write_processed_chunks(
                source=source,
                dataset=dataset,
                run_id=run_id,
                run_ts=run_ts,
                dataframe=dataframe,
                file_index=file_index,
            )
            processed_paths.extend(chunk_paths)
            total_rows += chunk_rows

            run_ctx.checkpoint("last_file", raw_path)
            logger.info(
                f"PROCESS FILE: source={source} dataset={dataset} raw_path={raw_path} "
                f"processed_files={len(chunk_paths)} rows={chunk_rows} bad_rows={file_bad_rows}"
            )

        if not raw_paths:
            logger.info(
                f"PROCESS NO INPUT: source={source} dataset={dataset} "
                "no raw parquet files were passed from raw manifest"
            )

        watermark_after = {
            "source": source,
            "dataset": dataset,
            "stage": "process",
            "mode": "file_incremental",
            "value": raw_paths[-1] if raw_paths else run_ctx.watermark_before.get("value"),
            "processed_files_count": len(processed_paths),
            "updated_at": run_ts,
            "run_id": run_id,
        }

        completed_manifest = run_ctx.complete(
            record_count=total_rows,
            raw_paths=raw_paths,
            processed_paths=processed_paths,
            bad_record_count=bad_rows,
            watermark_before=run_ctx.watermark_before,
            watermark_after=watermark_after,
        )
        if schema_columns:
            run_ctx.snapshot(columns=schema_columns, row_count=total_rows, captured_at=run_ts)

        run_ctx.checkpoint("status", "completed")
        logger.info(
            f"PROCESS COMPLETE: source={source} dataset={dataset} raw_files={len(raw_paths)} "
            f"processed_files={len(processed_paths)} rows={total_rows} bad_rows={bad_rows}"
        )
        return completed_manifest


@timed_run
def process_postgres_dataset_to_processed(
    table_cfg: PostgresTableConfig,
    run_id: str,
    run_ts: datetime,
    hint_raw_paths: list[str] | None = None,
) -> dict[str, Any]:
    spec = ProcessDatasetSpec(
        source="postgres",
        dataset=table_cfg.name,
        model_key=table_cfg.name,
        dedupe_keys=table_cfg.primary_key,
        dedupe_sort_column=table_cfg.watermark_column,
    )
    return _process_dataset(
        spec=spec,
        run_id=run_id,
        run_ts=run_ts,
        hint_raw_paths=hint_raw_paths,
    )


@timed_run
def process_file_dataset_to_processed(
    dataset_key: str,
    run_id: str,
    run_ts: datetime,
    hint_raw_paths: list[str] | None = None,
) -> dict[str, Any]:
    return _process_dataset(
        spec=FILE_PROCESS_SPECS[dataset_key],
        run_id=run_id,
        run_ts=run_ts,
        hint_raw_paths=hint_raw_paths,
    )


@timed_run
def process_marketing_to_processed(
    run_id: str,
    run_ts: datetime,
    hint_raw_paths: list[str] | None = None,
) -> dict[str, Any]:
    return process_file_dataset_to_processed("marketing_leads", run_id, run_ts, hint_raw_paths=hint_raw_paths)


@timed_run
def process_web_logs_to_processed(
    run_id: str,
    run_ts: datetime,
    hint_raw_paths: list[str] | None = None,
) -> dict[str, Any]:
    return process_file_dataset_to_processed("webserver_logs", run_id, run_ts, hint_raw_paths=hint_raw_paths)
