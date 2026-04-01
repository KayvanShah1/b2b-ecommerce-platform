from __future__ import annotations

from rich.console import Console

from b2b_ec_sources import get_connection

console = Console()

def reset_postgres_database() -> None:
    console.print("[bold]Resetting PostgreSQL tables...[/bold] Database is preserved.")
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """
                DROP TABLE IF EXISTS lead_conversions CASCADE;
                DROP TABLE IF EXISTS order_items CASCADE;
                DROP TABLE IF EXISTS orders CASCADE;
                DROP TABLE IF EXISTS products CASCADE;
                DROP TABLE IF EXISTS customers CASCADE;
                DROP TABLE IF EXISTS companies CASCADE;
                DROP TABLE IF EXISTS company_catalogs CASCADE;
                DROP TABLE IF EXISTS ref_countries CASCADE;
                """
            )

    console.print("[bold green]Done.[/bold green] Dropped source tables from Postgres.")


if __name__ == "__main__":
    reset_postgres_database()
