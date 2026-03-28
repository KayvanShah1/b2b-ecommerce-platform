from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.settings import settings
from b2b_ec_utils.storage import storage
from rich.console import Console

from b2b_ec_sources import get_connection

logger = get_logger("SourceDataReset")
console = Console()


def reset_database():
    """Drops all tables in the Postgres database."""
    conn = get_connection()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            logger.info("RESET: Dropping all existing database tables...")
            cur.execute("""
                DROP TABLE IF EXISTS order_items CASCADE;
                DROP TABLE IF EXISTS orders CASCADE;
                DROP TABLE IF EXISTS products CASCADE;
                DROP TABLE IF EXISTS customers CASCADE;
                DROP TABLE IF EXISTS companies CASCADE;
                DROP TABLE IF EXISTS company_catalogs CASCADE;
                DROP TABLE IF EXISTS ref_countries CASCADE;
            """)
            logger.info("RESET: Database is now empty and clean.")
    except Exception as e:
        logger.error(f"RESET: Could not reset database: {e}")
    finally:
        conn.close()


def reset_storage():
    """Clears the seed and daily folders from all configured buckets."""
    # List of buckets to clean
    buckets = [
        settings.storage.bucket,  # Warehouse/Orders
        settings.storage.webserver_logs_bucket,  # Weblogs
    ]

    for bucket in buckets:
        logger.info(f"RESET: Cleaning bucket: {bucket}...")
        try:
            # We target the root of the bucket/folder
            # storage.get_path helps us find the right protocol (s3:// or local var/)
            root_path = storage.get_path(bucket)

            # fsspec fs.rm with recursive=True is the cleanest way to wipe folders
            if storage.fs.exists(root_path):
                storage.fs.rm(root_path, recursive=True)
                logger.info(f"RESET: Successfully wiped {bucket}")
            else:
                logger.info(f"RESET: Bucket {bucket} was already empty.")

        except Exception as e:
            logger.error(f"RESET: Failed to clean bucket {bucket}: {e}")


if __name__ == "__main__":
    console.print("[bold red]🛑 WARNING: This action will WIPE the Database AND Storage Buckets![/bold red]")
    confirm = console.input("[bold red]Are you absolutely sure? (y/n): [/bold red]")

    if confirm.lower() == "y":
        # 1. Wipe DB
        reset_database()
        # 2. Wipe Storage
        reset_storage()
        console.print("[bold green]✅ System reset complete. Ready for a fresh seed.[/bold green]")
    else:
        console.print("[yellow]Reset cancelled.[/yellow]")
