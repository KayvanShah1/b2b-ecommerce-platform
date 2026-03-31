from __future__ import annotations

from b2b_ec_utils import settings
from b2b_ec_utils.storage import storage
from rich.console import Console
from rich.table import Table

console = Console()


def _bucket_root(bucket_name: str) -> str:
    return storage.get_path(bucket_name)


def _empty_bucket(bucket_name: str) -> tuple[int, bool]:
    root = _bucket_root(bucket_name)
    if not storage.fs.exists(root):
        return 0, False

    entries = storage.fs.ls(root, detail=False)
    deleted_count = 0
    for entry in entries:
        storage.fs.rm(entry, recursive=True)
        deleted_count += 1
    return deleted_count, True


def reset_minio_buckets(buckets) -> None:
    table = Table(title="MinIO Reset Summary")
    table.add_column("Bucket", style="cyan")
    table.add_column("Exists", style="yellow")
    table.add_column("Entries Deleted", justify="right", style="green")

    console.print("[bold]Resetting MinIO/S3 buckets (empty only, no bucket deletion)...[/bold]")
    for bucket in buckets:
        deleted_count, exists = _empty_bucket(bucket)
        table.add_row(bucket, "yes" if exists else "no", str(deleted_count))

    console.print(table)
    console.print("[bold green]Done.[/bold green] Buckets were emptied but not deleted.")


if __name__ == "__main__":
    buckets = [
        # settings.storage.webserver_logs_bucket,
        # settings.storage.marketing_leads_bucket,
        settings.storage.raw_data_bucket,
        settings.storage.metadata_bucket,
        settings.storage.processed_data_bucket,
    ]
    reset_minio_buckets(buckets=buckets)
