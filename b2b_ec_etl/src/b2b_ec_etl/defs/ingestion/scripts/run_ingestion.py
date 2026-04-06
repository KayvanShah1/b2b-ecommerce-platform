from datetime import datetime, timezone

from b2b_ec_etl.defs.ingestion.scripts.load_runner import run_load_only
from b2b_ec_etl.defs.ingestion.scripts.raw_capture_runner import run_process_only, run_raw_capture
from b2b_ec_utils.timer import timed_run


@timed_run()
def run_ingestion_etl(run_id: str | None = None):
    run_ts = datetime.now(timezone.utc)
    effective_run_id = run_id or f"manual-ingestion-{run_ts.strftime('%Y%m%dT%H%M%S')}"

    run_raw_capture(run_id=effective_run_id)
    run_process_only(run_id=effective_run_id)
    run_load_only(run_id=effective_run_id)


if __name__ == "__main__":
    run_ingestion_etl()
