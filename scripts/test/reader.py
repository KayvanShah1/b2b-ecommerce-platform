import sys

import pandas as pd
from b2b_ec_utils.storage import storage

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def _latest_path(pattern: str) -> str | None:
    files = sorted(storage.glob(pattern))
    return files[-1] if files else None


def _print_parquet_head(path: str, rows: int = 5) -> None:
    with storage.open(path, mode="rb") as file_handle:
        dataframe = pd.read_parquet(file_handle)
    print(f"\nFILE: {path}")
    print(dataframe.head(rows).to_string(index=False))


def peek_latest_raw(source: str, dataset: str, rows: int = 5) -> None:
    pattern = storage.get_raw_dataset_path(
        source,
        f"dataset={dataset}",
        "run_date=*",
        "run_hour=*",
        "run_id=*",
        "*.parquet",
    )
    latest = _latest_path(pattern)
    if not latest:
        print(f"\nNo raw parquet found for source={source} dataset={dataset}")
        return
    _print_parquet_head(latest, rows=rows)


def peek_latest_processed(source: str, dataset: str, rows: int = 5) -> None:
    pattern = storage.get_processed_dataset_path(
        source,
        f"dataset={dataset}",
        "run_date=*",
        "run_hour=*",
        "run_id=*",
        "*.parquet",
    )
    latest = _latest_path(pattern)
    if not latest:
        print(f"\nNo processed parquet found for source={source} dataset={dataset}")
        return
    _print_parquet_head(latest, rows=rows)


if __name__ == "__main__":
    # Raw
    # peek_latest_raw(source="marketing_leads", dataset="marketing_leads")
    # peek_latest_raw(source="webserver_logs", dataset="webserver_logs")

    # # Processed
    peek_latest_processed(source="marketing_leads", dataset="marketing_leads")
    # peek_latest_processed(source="webserver_logs", dataset="webserver_logs")
