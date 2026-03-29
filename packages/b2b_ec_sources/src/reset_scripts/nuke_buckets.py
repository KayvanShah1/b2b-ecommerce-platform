from b2b_ec_utils.storage import storage
from rich.console import Console

console = Console()


def nuke(bucket_name: str = None):
    console.print(f"[red]Deleting[/red] '{bucket_name}' bucket...")
    if not storage.fs.exists(bucket_name):
        console.print(f"[yellow]Warning:[/yellow] Bucket '{bucket_name}' does not exist. Nothing to delete.")
        return
    storage.fs.rm(bucket_name, recursive=True)
    console.print(f"[green]Success[/green] Bucket '{bucket_name}' has been deleted.")


if __name__ == "__main__":
    nuke("b2b-ec-marketing-leads")
