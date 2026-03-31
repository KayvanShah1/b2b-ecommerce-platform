from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from b2b_ec_etl.defs.ingestion.resources import IngestionResource
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.timer import timed_run

logger = get_logger("RawCaptureRunner")


@timed_run
def run_raw_capture(run_id: str | None = None) -> dict[str, Any]:
    run_ts = datetime.now(timezone.utc)
    effective_run_id = run_id or f"manual-raw-{run_ts.strftime('%Y%m%dT%H%M%S')}"

    resource = IngestionResource()
    result = resource.raw_capture_all(run_id=effective_run_id, run_ts=run_ts)

    postgres_rows = sum(m["record_count"] for m in result["postgres"])
    file_rows = sum(m["record_count"] for m in result["files"].values())
    postgres_skipped = sum(1 for m in result["postgres"] if m.get("record_count", 0) == 0)
    file_skipped = sum(1 for m in result["files"].values() if m.get("record_count", 0) == 0)
    logger.info(
        f"RAW CAPTURE COMPLETE: run_id={effective_run_id} "
        f"postgres_datasets={len(result['postgres'])} postgres_rows={postgres_rows} postgres_skipped={postgres_skipped} "
        f"file_datasets={len(result['files'])} file_rows={file_rows} file_skipped={file_skipped}"
    )
    return result


@timed_run
def run_raw_capture_and_process(run_id: str | None = None) -> dict[str, Any]:
    run_ts = datetime.now(timezone.utc)
    effective_run_id = run_id or f"manual-ingestion-{run_ts.strftime('%Y%m%dT%H%M%S')}"

    resource = IngestionResource()
    raw_result = resource.raw_capture_all(run_id=effective_run_id, run_ts=run_ts)
    process_result = resource.process_all(run_id=effective_run_id, run_ts=run_ts, raw_result=raw_result)

    raw_postgres_rows = sum(m["record_count"] for m in raw_result["postgres"])
    raw_file_rows = sum(m["record_count"] for m in raw_result["files"].values())
    process_postgres_rows = sum(m["record_count"] for m in process_result["postgres"])
    process_file_rows = sum(m["record_count"] for m in process_result["files"].values())
    process_bad_rows = sum(m.get("bad_record_count", 0) for m in process_result["postgres"]) + sum(
        m.get("bad_record_count", 0) for m in process_result["files"].values()
    )

    logger.info(
        f"RAW+PROCESS COMPLETE: run_id={effective_run_id} "
        f"raw_postgres_rows={raw_postgres_rows} raw_file_rows={raw_file_rows} "
        f"processed_postgres_rows={process_postgres_rows} processed_file_rows={process_file_rows} "
        f"process_bad_rows={process_bad_rows}"
    )
    return {"run_id": effective_run_id, "raw": raw_result, "processed": process_result}


@timed_run
def run_process_only(run_id: str | None = None) -> dict[str, Any]:
    run_ts = datetime.now(timezone.utc)
    effective_run_id = run_id or f"manual-process-{run_ts.strftime('%Y%m%dT%H%M%S')}"

    resource = IngestionResource()
    process_result = resource.process_all(run_id=effective_run_id, run_ts=run_ts, raw_result=None)

    process_postgres_rows = sum(m["record_count"] for m in process_result["postgres"])
    process_file_rows = sum(m["record_count"] for m in process_result["files"].values())
    process_bad_rows = sum(m.get("bad_record_count", 0) for m in process_result["postgres"]) + sum(
        m.get("bad_record_count", 0) for m in process_result["files"].values()
    )
    logger.info(
        f"PROCESS ONLY COMPLETE: run_id={effective_run_id} "
        f"processed_postgres_rows={process_postgres_rows} "
        f"processed_file_rows={process_file_rows} process_bad_rows={process_bad_rows}"
    )
    return {"run_id": effective_run_id, "processed": process_result}


if __name__ == "__main__":
    run_raw_capture()
