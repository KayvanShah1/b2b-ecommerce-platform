import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import polars as pl
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.storage import storage
from b2b_ec_utils.timer import timed_run

from b2b_ec_pipeline.ingestion.io import schema_columns as build_schema_columns, write_parquet_frame
from b2b_ec_pipeline.ingestion.models import FILE_RAW_CAPTURE_SPECS, RawFileCaptureSpec
from b2b_ec_pipeline.state import IngestionRunContext, RunManifest, managed_ingestion_run, state_manager

logger = get_logger("FileRawIngestion")
WEB_LOGS_CHUNK_SIZE = 50_000
WEB_LOGS_READ_BLOCK_SIZE = 8 * 1024 * 1024


def _marketing_csv_pattern() -> str:
    return storage.get_marketing_leads_path("b2b_leads_*.csv")


def _web_logs_seed_pattern() -> str:
    return storage.get_webserver_logs_path(True, "*.jsonl")


def _web_logs_daily_pattern() -> str:
    return storage.get_webserver_logs_path(False, "*.jsonl")


PATTERN_RESOLVERS: dict[str, Callable[[], str]] = {
    "marketing_csv": _marketing_csv_pattern,
    "web_logs_seed_jsonl": _web_logs_seed_pattern,
    "web_logs_daily_jsonl": _web_logs_daily_pattern,
}

SOURCE_FILE_TS_PATTERN = re.compile(r"(\d{8}_\d{6})")


def _read_marketing_csv(path: str, run_id: str, run_ts: datetime) -> pl.DataFrame:
    with storage.open(path, mode="rb") as file_handle:
        dataframe = pl.read_csv(file_handle)
    return dataframe.with_columns(
        [
            pl.lit(path).alias("_source_file"),
            pl.lit(run_id).alias("_ingestion_run_id"),
            pl.lit(run_ts.isoformat()).alias("_ingested_at"),
        ]
    )


def _iter_web_logs_jsonl_chunks(path: str, run_id: str, run_ts: datetime):
    rows: list[dict[str, Any]] = []
    bad_lines = 0

    def _append_parsed(raw_line: bytes, line_idx: int) -> bool:
        nonlocal rows
        line = raw_line.decode("utf-8", errors="replace").strip()
        if not line:
            return True
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            logger.warning(f"Skipping invalid JSONL row: file={path} line={line_idx}")
            return False

        rows.append(
            {
                "event_id": payload.get("event_id") or f"{path}:{line_idx}",
                "remote_host": payload.get("remote_host"),
                "username": payload.get("username"),
                "event_ts": payload.get("timestamp"),
                "user_agent": payload.get("user_agent"),
                "request_path": payload.get("request_path"),
                "status_code": payload.get("status_code"),
                "_source_file": path,
                "_source_line": line_idx,
                "_ingestion_run_id": run_id,
                "_ingested_at": run_ts.isoformat(),
            }
        )
        return True

    pending = b""
    line_idx = -1
    with storage.open(path, mode="rb", block_size=WEB_LOGS_READ_BLOCK_SIZE) as file_handle:
        while True:
            block = file_handle.read(WEB_LOGS_READ_BLOCK_SIZE)
            if not block:
                break
            pending += block
            lines = pending.split(b"\n")
            pending = lines.pop()

            for raw_line in lines:
                line_idx += 1
                if not _append_parsed(raw_line, line_idx):
                    bad_lines += 1
                if len(rows) >= WEB_LOGS_CHUNK_SIZE:
                    yield pl.DataFrame(rows), bad_lines
                    rows = []
                    bad_lines = 0

        if pending:
            line_idx += 1
            if not _append_parsed(pending, line_idx):
                bad_lines += 1

    if rows or bad_lines:
        yield (pl.DataFrame(rows) if rows else pl.DataFrame()), bad_lines


def _resolve_patterns(spec: RawFileCaptureSpec) -> list[str]:
    return [PATTERN_RESOLVERS[key]() for key in spec.pattern_keys]


def _source_file_ts(path: str) -> str | None:
    match = SOURCE_FILE_TS_PATTERN.search(Path(path).name)
    return match.group(1) if match else None


def _source_sort_key(path: str) -> tuple[str, str]:
    # Keep unknown timestamps at the end; they are still processed via tiebreaker path ordering.
    ts_key = _source_file_ts(path) or "99999999_999999"
    return (ts_key, path)


def _normalize_cursor(value: str | None, last_file: str | None) -> tuple[str | None, str | None]:
    if value and SOURCE_FILE_TS_PATTERN.fullmatch(value):
        return value, last_file
    if last_file:
        inferred = _source_file_ts(last_file)
        if inferred:
            return inferred, last_file
    return None, last_file


def _is_new_file(path: str, cursor_ts: str | None, cursor_file: str | None) -> bool:
    ts_key = _source_file_ts(path)
    if cursor_ts is None:
        return True if cursor_file is None else path > cursor_file
    if ts_key is None:
        logger.warning(f"FILE RAW DISCOVERY: timestamp missing in file name, using path fallback: {path}")
        return True if cursor_file is None else path > cursor_file
    if ts_key > cursor_ts:
        return True
    if ts_key < cursor_ts:
        return False
    return True if cursor_file is None else path > cursor_file


def _collect_new_files(
    patterns: list[str],
    cursor_ts: str | None,
    cursor_file: str | None,
) -> tuple[list[str], list[str]]:
    discovered = sorted(set(path for pattern in patterns for path in storage.glob(pattern)), key=_source_sort_key)
    new_files = [path for path in discovered if _is_new_file(path, cursor_ts, cursor_file)]
    return discovered, new_files


def _raw_output_path(source_key: str, dataset: str, source_file: str, run_id: str, run_ts: datetime, suffix: str) -> str:
    return storage.get_raw_dataset_path(
        source_key,
        f"dataset={dataset}",
        f"run_date={run_ts.strftime('%Y-%m-%d')}",
        f"run_hour={run_ts.strftime('%H')}",
        f"{run_id}_{Path(source_file).stem}{suffix}.parquet",
    )


def _capture_marketing_files(
    spec: RawFileCaptureSpec,
    run_ctx: IngestionRunContext,
    run_id: str,
    run_ts: datetime,
    source_files: list[str],
) -> tuple[list[str], int, list[dict[str, Any]], int]:
    output_paths: list[str] = []
    total_rows = 0
    columns: list[dict[str, Any]] = []

    for source_file in source_files:
        frame = _read_marketing_csv(source_file, run_id, run_ts)
        if frame.is_empty():
            run_ctx.checkpoint("last_file", source_file)
            continue

        output_path = _raw_output_path(spec.source, spec.dataset, source_file, run_id, run_ts, "")
        write_parquet_frame(output_path, frame)

        output_paths.append(output_path)
        total_rows += frame.height
        if not columns:
            columns = build_schema_columns(frame)
        run_ctx.checkpoint("last_file", source_file)

    return output_paths, total_rows, columns, 0


def _capture_web_logs_files(
    spec: RawFileCaptureSpec,
    run_ctx: IngestionRunContext,
    run_id: str,
    run_ts: datetime,
    source_files: list[str],
) -> tuple[list[str], int, list[dict[str, Any]], int]:
    output_paths: list[str] = []
    total_rows = 0
    total_bad_lines = 0
    columns: list[dict[str, Any]] = []

    for source_file in source_files:
        chunk_index = 0
        for frame, bad_lines in _iter_web_logs_jsonl_chunks(source_file, run_id, run_ts):
            total_bad_lines += bad_lines
            if frame.is_empty():
                continue

            output_path = _raw_output_path(
                spec.source, spec.dataset, source_file, run_id, run_ts, f"-part-{chunk_index:05d}"
            )
            write_parquet_frame(output_path, frame)

            output_paths.append(output_path)
            total_rows += frame.height
            if not columns:
                columns = build_schema_columns(frame)

            logger.info(
                f"FILE RAW CHUNK: dataset={spec.dataset} source={source_file} "
                f"chunk={chunk_index} rows={frame.height} output={output_path}"
            )
            chunk_index += 1

        run_ctx.checkpoint("last_file", source_file)

    return output_paths, total_rows, columns, total_bad_lines


CAPTURE_HANDLERS: dict[str, Callable[..., tuple[list[str], int, list[dict[str, Any]], int]]] = {
    "marketing_csv": _capture_marketing_files,
    "web_logs_jsonl": _capture_web_logs_files,
}


def _ingest_file_source(spec: RawFileCaptureSpec, run_id: str, run_ts: datetime) -> RunManifest:
    with managed_ingestion_run(
        state_manager=state_manager,
        run_id=run_id,
        source=spec.source,
        dataset=spec.dataset,
        run_ts=run_ts,
        stage="raw_capture",
    ) as run_ctx:
        previous_ts, previous_file = _normalize_cursor(
            run_ctx.watermark_before.get("value"),
            run_ctx.watermark_before.get("last_file"),
        )
        handler = CAPTURE_HANDLERS[spec.loader]

        discovered_files, new_files = _collect_new_files(_resolve_patterns(spec), previous_ts, previous_file)
        skipped_count = len(discovered_files) - len(new_files)
        latest_new_file = new_files[-1] if new_files else previous_file
        latest_new_ts = (_source_file_ts(latest_new_file) if latest_new_file else None) or previous_ts
        logger.info(
            f"FILE RAW DISCOVERY: source={spec.source} discovered={len(discovered_files)} "
            f"new={len(new_files)} skipped={skipped_count} wm_before_ts={previous_ts} wm_before_file={previous_file}"
        )

        output_paths, total_rows, schema_columns, bad_lines = handler(spec, run_ctx, run_id, run_ts, new_files)
        watermark_after = {
            "source": spec.source,
            "dataset": spec.dataset,
            "stage": "raw_capture",
            "mode": "file_incremental",
            "value": latest_new_ts,
            "last_file": latest_new_file,
            "processed_files_count": len(new_files),
            "updated_at": run_ts,
            "run_id": run_id,
        }

        completed_manifest = run_ctx.complete(
            record_count=total_rows,
            bad_record_count=bad_lines,
            processed_files=new_files,
            raw_paths=output_paths,
            watermark_before=run_ctx.watermark_before,
            watermark_after=watermark_after,
        )
        if schema_columns:
            run_ctx.snapshot(columns=schema_columns, row_count=total_rows, captured_at=run_ts)
        run_ctx.checkpoint("status", "completed")

        logger.info(
            f"FILE RAW: source={spec.source} files={len(new_files)} rows={total_rows} "
            f"bad_lines={bad_lines} "
            f"wm_before_ts={previous_ts} wm_after_ts={watermark_after['value']} "
            f"wm_after_file={watermark_after['last_file']}"
        )
        if not new_files:
            logger.info(
                f"FILE RAW NO CHANGES: source={spec.source} "
                f"no new files after cursor_ts={previous_ts} cursor_file={previous_file}"
            )
        return completed_manifest


@timed_run
def ingest_file_source_to_raw(dataset_key: str, run_id: str, run_ts: datetime) -> RunManifest:
    return _ingest_file_source(FILE_RAW_CAPTURE_SPECS[dataset_key], run_id, run_ts)
