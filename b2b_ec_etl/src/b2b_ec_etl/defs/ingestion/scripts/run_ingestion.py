from b2b_ec_etl.defs.ingestion.scripts.load_runner import run_load_only
from b2b_ec_etl.defs.ingestion.scripts.raw_capture_runner import run_process_only, run_raw_capture


def run():
    run_raw_capture()
    run_process_only()
    run_load_only()


if __name__ == "__main__":
    run()
