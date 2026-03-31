from __future__ import annotations

from rich.console import Console
from rich.table import Table

from b2b_ec_utils.storage import storage

console = Console()


def delete_buckets(bucket_names: list[str], ask_confirmation: bool = True) -> None:
    if not bucket_names:
        console.print("[yellow]No bucket names provided. Nothing to do.[/yellow]")
        return

    if ask_confirmation:
        console.print(
            "[bold red]This will delete the selected buckets (or bucket roots for object storage).[/bold red]"
        )
        console.print(f"[red]Targets:[/red] {', '.join(bucket_names)}")
        confirm = console.input("[bold red]Proceed? (y/n): [/bold red]").strip().lower()
        if confirm != "y":
            console.print("[yellow]Cancelled.[/yellow]")
            return

    summary = Table(title="Bucket Deletion Summary")
    summary.add_column("Bucket", style="cyan")
    summary.add_column("Status", style="green")
    summary.add_column("Details", style="yellow")

    for bucket_name in bucket_names:
        root = storage.get_path(bucket_name)
        try:
            if not storage.fs.exists(root):
                summary.add_row(bucket_name, "skipped", "Bucket/root does not exist")
                continue

            storage.fs.rm(root, recursive=True)

            try:
                storage.fs.rmdir(root)
                summary.add_row(bucket_name, "deleted", "Objects removed and root deleted")
            except Exception:
                summary.add_row(bucket_name, "partial", "Objects removed; root delete not supported by backend")
        except Exception as exc:
            summary.add_row(bucket_name, "failed", str(exc))

    console.print(summary)


if __name__ == "__main__":
    buckets_to_delete = [
        "b2b-ec-webserver-logs",
        "b2b-ec-marketing-leads",
        "b2b-ec-raw-data",
        "b2b-ec-metadata",
        "b2b-ec-processed-data",
    ]
    delete_buckets(buckets_to_delete, ask_confirmation=True)
