from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import polars as pl
from b2b_ec_sources import get_connection
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.storage import storage
from b2b_ec_utils.timer import timed_run

from b2b_ec_pipeline.ingestion.io import write_parquet_frame
from b2b_ec_pipeline.ingestion.models import PostgresTableConfig
from b2b_ec_pipeline.state import IngestionRunContext, RunManifest, managed_ingestion_run, state_manager

logger = get_logger("PostgresRawIngestion")
FETCH_BATCH_SIZE = 50_000


def _normalize_value(value: Any) -> Any:
    return float(value) if isinstance(value, Decimal) else value


def _coerce_previous_watermark(table_cfg: PostgresTableConfig, value: Any) -> Any:
    if value is None:
        return None
    if table_cfg.mode == "incremental_id" and isinstance(value, str):
        return int(value)
    if table_cfg.mode == "incremental_timestamp" and isinstance(value, str):
        return datetime.fromisoformat(value)
    return value


def _compute_high_watermark(cur, table_cfg: PostgresTableConfig) -> Any:
    if table_cfg.mode == "full_snapshot":
        return datetime.now(timezone.utc)
    assert table_cfg.watermark_column
    cur.execute(f"SELECT MAX({table_cfg.watermark_column}) FROM {table_cfg.name}")
    return cur.fetchone()[0]


def _execute_select(cur, table_cfg: PostgresTableConfig, low_wm: Any, high_wm: Any) -> list[str]:
    if table_cfg.mode == "full_snapshot":
        cur.execute(f"SELECT * FROM {table_cfg.name}")
    elif high_wm is None:
        cur.execute(f"SELECT * FROM {table_cfg.name} WHERE 1=0")
    elif low_wm is None:
        cur.execute(
            f"SELECT * FROM {table_cfg.name} WHERE {table_cfg.watermark_column} <= %s ORDER BY {table_cfg.watermark_column}",
            (high_wm,),
        )
    else:
        cur.execute(
            f"""
            SELECT * FROM {table_cfg.name}
            WHERE {table_cfg.watermark_column} > %s AND {table_cfg.watermark_column} <= %s
            ORDER BY {table_cfg.watermark_column}
            """,
            (low_wm, high_wm),
        )
    return [column[0] for column in cur.description]


def _log_extract_plan(table_cfg: PostgresTableConfig, previous_value: Any, high_watermark: Any) -> None:
    if table_cfg.mode == "full_snapshot":
        action = "recapture_all"
    elif high_watermark is None:
        action = "skip_empty_source"
    elif previous_value is not None and high_watermark <= previous_value:
        action = "skip_no_changes"
    else:
        action = "extract_increment"

    logger.info(
        f"POSTGRES RAW PLAN: dataset={table_cfg.name} mode={table_cfg.mode} "
        f"wm_before={previous_value} wm_high={high_watermark} action={action}"
    )


def _schema_columns(dataframe: pl.DataFrame) -> list[dict[str, Any]]:
    return [{"name": name, "dtype": str(dtype), "nullable": True} for name, dtype in dataframe.schema.items()]


def _chunk_output_path(dataset: str, run_id: str, run_ts: datetime, chunk_index: int) -> str:
    return storage.get_raw_dataset_path(
        "postgres",
        f"dataset={dataset}",
        f"run_date={run_ts.strftime('%Y-%m-%d')}",
        f"run_hour={run_ts.strftime('%H')}",
        f"{run_id}_part-{chunk_index:05d}.parquet",
    )


def _write_cursor_chunks(
    cur,
    columns: list[str],
    table_cfg: PostgresTableConfig,
    run_ctx: IngestionRunContext,
    run_id: str,
    run_ts: datetime,
) -> tuple[list[str], int, list[dict[str, Any]]]:
    raw_paths: list[str] = []
    row_count = 0
    schema_columns: list[dict[str, Any]] = []
    chunk_index = 0

    while True:
        rows = cur.fetchmany(FETCH_BATCH_SIZE)
        if not rows:
            break

        dataframe = pl.DataFrame([tuple(_normalize_value(value) for value in row) for row in rows], schema=columns, orient="row")
        if not schema_columns:
            schema_columns = _schema_columns(dataframe)

        output_path = _chunk_output_path(table_cfg.name, run_id, run_ts, chunk_index)
        write_parquet_frame(output_path, dataframe)

        raw_paths.append(output_path)
        row_count += len(rows)
        run_ctx.checkpoint("last_chunk", chunk_index)
        logger.info(
            f"POSTGRES RAW CHUNK: dataset={table_cfg.name} chunk={chunk_index} rows={len(rows)} output={output_path}"
        )
        chunk_index += 1

    return raw_paths, row_count, schema_columns


@timed_run
def extract_postgres_table_to_raw(table_cfg: PostgresTableConfig, run_id: str, run_ts: datetime) -> RunManifest:
    raw_paths: list[str] = []
    row_count = 0
    schema_columns: list[dict[str, Any]] = []
    with managed_ingestion_run(
        state_manager=state_manager,
        run_id=run_id,
        source="postgres",
        dataset=table_cfg.name,
        run_ts=run_ts,
        stage="raw_capture",
        failure_context=lambda: {
            "record_count": row_count,
            "raw_paths": raw_paths,
        },
    ) as run_ctx:
        previous_value = _coerce_previous_watermark(table_cfg, run_ctx.watermark_before.get("value"))
        with get_connection() as conn:
            with conn.cursor() as cur:
                high_watermark = _compute_high_watermark(cur, table_cfg)
                _log_extract_plan(table_cfg, previous_value, high_watermark)
                columns = _execute_select(cur, table_cfg, previous_value, high_watermark)
                raw_paths, row_count, schema_columns = _write_cursor_chunks(
                    cur=cur,
                    columns=columns,
                    table_cfg=table_cfg,
                    run_ctx=run_ctx,
                    run_id=run_id,
                    run_ts=run_ts,
                )

        watermark_after = {
            "source": "postgres",
            "dataset": table_cfg.name,
            "stage": "raw_capture",
            "mode": table_cfg.mode,
            "watermark_column": table_cfg.watermark_column,
            "value": high_watermark if high_watermark is not None else previous_value,
            "updated_at": run_ts,
            "run_id": run_id,
        }
        completed_manifest = run_ctx.complete(
            record_count=row_count,
            raw_paths=raw_paths,
            watermark_before=run_ctx.watermark_before,
            watermark_after=watermark_after,
        )
        if schema_columns:
            run_ctx.snapshot(columns=schema_columns, row_count=row_count, captured_at=run_ts)
        run_ctx.checkpoint("status", "completed")

        logger.info(
            f"POSTGRES RAW: dataset={table_cfg.name} rows={row_count} files={len(raw_paths)} "
            f"wm_before={previous_value} wm_after={watermark_after['value']}"
        )
        if row_count == 0:
            logger.info(
                f"POSTGRES RAW NO CHANGES: dataset={table_cfg.name} mode={table_cfg.mode} "
                f"wm_before={previous_value} wm_high={high_watermark}"
            )
        return completed_manifest
