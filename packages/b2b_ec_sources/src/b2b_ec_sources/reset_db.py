import psycopg2
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.settings import settings
from rich.console import Console

logger = get_logger("DatabaseReset")
console = Console()


def reset_database():
    conn = psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        user=settings.postgres.user,
        password=settings.postgres.password,
        database=settings.postgres.database,
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            # Using logger for the "audit trail"
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

            # Using logger for the "user interface"
            logger.info("RESET: Database is now empty and clean.")

    except Exception as e:
        logger.error(f"RESET: Could not reset database: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    # This renders the prompt in bold red
    console.print("[bold red]WARNING: This action will WIPE the entire database![/bold red]")
    confirm = console.input("[bold red]Are you sure you want to proceed? (y/n): [/bold red]")

    if confirm.lower() == "y":
        reset_database()
    else:
        console.print("[yellow]Reset cancelled.[/yellow]")
