from b2b_ec_utils.logger import get_logger
from rich.console import Console

from b2b_ec_sources import get_connection

logger = get_logger("DatabaseReset")
console = Console()


def reset_database():
    conn = get_connection()
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            logger.info("RESET: Dropping all existing tables...")

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


if __name__ == "__main__":
    console.print("[bold red]WARNING: This action will WIPE the entire database![/bold red]")
    confirm = console.input("[bold red]Are you sure you want to proceed? (y/n): [/bold red]")

    if confirm.lower() == "y":
        reset_database()
    else:
        console.print("[yellow]Reset cancelled.[/yellow]")
