"""Validate source DB integrity and geography consistency of generated artifacts."""

import argparse
import json
from contextlib import contextmanager

import polars as pl
from b2b_ec_sources import get_connection
from b2b_ec_sources.geography import GEO
from b2b_ec_utils.storage import storage
from rich.console import Console
from rich.table import Table

console = Console()
ALLOWED_LEAD_STATUSES = {"New", "Contacted", "Qualified", "Lost", "Nurturing", "Converted"}


def _latest_path(pattern: str) -> str | None:
    files = storage.glob(pattern)
    if not files:
        return None
    return sorted(files)[-1]


def _print_top_country_share(label: str, country_codes: list[str]) -> None:
    if not country_codes:
        console.print(f"[yellow]{label}: no rows[/yellow]")
        return

    top_set = {c.upper() for c in GEO.top_country_codes}
    total = len(country_codes)
    top_count = sum(1 for c in country_codes if (c or "").upper() in top_set)
    share = top_count / total
    tone = "green" if share >= GEO.top_country_share * 0.9 else "yellow"
    console.print(
        f"{label}: top-10 share = [{tone}]{share:.2%}[/{tone}] "
        f"({top_count}/{total}) [target={GEO.top_country_share:.0%}]"
    )


@contextmanager
def _db_cursor():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def validate_source_db() -> None:
    required_tables = [
        "ref_countries",
        "companies",
        "products",
        "company_catalogs",
        "customers",
        "orders",
        "order_items",
    ]

    console.rule("[bold]Source DB Validation[/bold]")
    with _db_cursor() as cur:
        missing_tables: list[str] = []
        for table in required_tables:
            cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
            if cur.fetchone()[0] is None:
                missing_tables.append(table)

        if missing_tables:
            console.print(f"[red]FAIL:[/red] Missing required tables: {', '.join(missing_tables)}")
            return

        counts = {}
        for table in required_tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]

        cur.execute("SELECT to_regclass('public.lead_conversions')")
        has_lead_conversions = cur.fetchone()[0] is not None
        if has_lead_conversions:
            cur.execute("SELECT COUNT(*) FROM lead_conversions")
            counts["lead_conversions"] = cur.fetchone()[0]

        row_count_table = Table(title="Row Counts", show_header=True, header_style="bold cyan")
        row_count_table.add_column("Table")
        row_count_table.add_column("Rows", justify="right")
        for table_name in required_tables:
            row_count_table.add_row(table_name, f"{counts[table_name]}")
        if has_lead_conversions:
            row_count_table.add_row("lead_conversions (optional)", f"{counts['lead_conversions']}")
        else:
            row_count_table.add_row("lead_conversions (optional)", "missing")
        console.print(row_count_table)

        if has_lead_conversions:
            console.print("[dim]Optional conversion table detected; running conversion checks.[/dim]")
        else:
            console.print("[dim]Optional table missing: lead_conversions (conversion checks skipped).[/dim]")

        checks = [
            (
                "companies.country_code exists in ref_countries",
                """
                    SELECT COUNT(*)
                    FROM companies c
                    LEFT JOIN ref_countries rc ON c.country_code = rc.code
                    WHERE rc.code IS NULL
                    """,
            ),
            (
                "companies.type is valid",
                """
                    SELECT COUNT(*)
                    FROM companies
                    WHERE type IS NULL OR type NOT IN ('Supplier', 'Client')
                    """,
            ),
            (
                "products.supplier_cuit references companies",
                """
                    SELECT COUNT(*)
                    FROM products p
                    LEFT JOIN companies c ON p.supplier_cuit = c.cuit
                    WHERE c.cuit IS NULL
                    """,
            ),
            (
                "products owned by Supplier companies",
                """
                    SELECT COUNT(*)
                    FROM products p
                    JOIN companies c ON p.supplier_cuit = c.cuit
                    WHERE c.type <> 'Supplier'
                    """,
            ),
            (
                "customers.company_cuit references companies",
                """
                    SELECT COUNT(*)
                    FROM customers cu
                    LEFT JOIN companies c ON cu.company_cuit = c.cuit
                    WHERE c.cuit IS NULL
                    """,
            ),
            (
                "customers belong to Client companies",
                """
                    SELECT COUNT(*)
                    FROM customers cu
                    JOIN companies c ON cu.company_cuit = c.cuit
                    WHERE c.type <> 'Client'
                    """,
            ),
            (
                "company_catalogs.company_cuit references companies",
                """
                    SELECT COUNT(*)
                    FROM company_catalogs cc
                    LEFT JOIN companies c ON cc.company_cuit = c.cuit
                    WHERE c.cuit IS NULL
                    """,
            ),
            (
                "company_catalogs company is Client",
                """
                    SELECT COUNT(*)
                    FROM company_catalogs cc
                    JOIN companies c ON cc.company_cuit = c.cuit
                    WHERE c.type <> 'Client'
                    """,
            ),
            (
                "company_catalogs.product_id references products",
                """
                    SELECT COUNT(*)
                    FROM company_catalogs cc
                    LEFT JOIN products p ON cc.product_id = p.id
                    WHERE p.id IS NULL
                    """,
            ),
            (
                "orders.customer_id references customers",
                """
                    SELECT COUNT(*)
                    FROM orders o
                    LEFT JOIN customers cu ON o.customer_id = cu.id
                    WHERE cu.id IS NULL
                    """,
            ),
            (
                "orders.company_cuit references companies",
                """
                    SELECT COUNT(*)
                    FROM orders o
                    LEFT JOIN companies c ON o.company_cuit = c.cuit
                    WHERE c.cuit IS NULL
                    """,
            ),
            (
                "orders.company_cuit matches customers.company_cuit",
                """
                    SELECT COUNT(*)
                    FROM orders o
                    JOIN customers cu ON o.customer_id = cu.id
                    WHERE o.company_cuit <> cu.company_cuit
                    """,
            ),
            (
                "orders.status is valid",
                """
                    SELECT COUNT(*)
                    FROM orders
                    WHERE status IS NULL OR status NOT IN ('COMPLETED', 'CANCELLED', 'RETURNED')
                    """,
            ),
            (
                "orders.total_amount is positive",
                """
                    SELECT COUNT(*)
                    FROM orders
                    WHERE COALESCE(total_amount, 0) <= 0
                    """,
            ),
            (
                "order_items.order_id references orders",
                """
                    SELECT COUNT(*)
                    FROM order_items oi
                    LEFT JOIN orders o ON oi.order_id = o.id
                    WHERE o.id IS NULL
                    """,
            ),
            (
                "order_items.product_id references products",
                """
                    SELECT COUNT(*)
                    FROM order_items oi
                    LEFT JOIN products p ON oi.product_id = p.id
                    WHERE p.id IS NULL
                    """,
            ),
            (
                "order_items.quantity is positive",
                """
                    SELECT COUNT(*)
                    FROM order_items
                    WHERE COALESCE(quantity, 0) <= 0
                    """,
            ),
            (
                "order_items.unit_price is positive",
                """
                    SELECT COUNT(*)
                    FROM order_items
                    WHERE COALESCE(unit_price, 0) <= 0
                    """,
            ),
            (
                "ordered products exist in buyer catalog",
                """
                    SELECT COUNT(*)
                    FROM order_items oi
                    JOIN orders o ON oi.order_id = o.id
                    JOIN customers cu ON o.customer_id = cu.id
                    LEFT JOIN company_catalogs cc
                      ON cc.company_cuit = cu.company_cuit
                     AND cc.product_id = oi.product_id
                    WHERE cc.product_id IS NULL
                    """,
            ),
            (
                "orders.total_amount matches sum(order_items)",
                """
                    WITH item_totals AS (
                        SELECT order_id, ROUND(SUM(quantity * unit_price)::numeric, 2) AS items_total
                        FROM order_items
                        GROUP BY order_id
                    )
                    SELECT COUNT(*)
                    FROM orders o
                    JOIN item_totals it ON o.id = it.order_id
                    WHERE ABS(o.total_amount - it.items_total) > 0.05
                    """,
            ),
            (
                "customers.username is unique",
                """
                    SELECT COUNT(*)
                    FROM (
                        SELECT username
                        FROM customers
                        GROUP BY username
                        HAVING COUNT(*) > 1
                    ) t
                    """,
            ),
            (
                "customers.email is unique",
                """
                    SELECT COUNT(*)
                    FROM (
                        SELECT email
                        FROM customers
                        GROUP BY email
                        HAVING COUNT(*) > 1
                    ) t
                    """,
            ),
        ]

        if has_lead_conversions:
            checks.extend(
                [
                    (
                        "lead_conversions.company_cuit references companies",
                        """
                            SELECT COUNT(*)
                            FROM lead_conversions lc
                            LEFT JOIN companies c ON lc.company_cuit = c.cuit
                            WHERE c.cuit IS NULL
                            """,
                    ),
                    (
                        "lead_conversions company is Client",
                        """
                            SELECT COUNT(*)
                            FROM lead_conversions lc
                            JOIN companies c ON lc.company_cuit = c.cuit
                            WHERE c.type <> 'Client'
                            """,
                    ),
                    (
                        "lead_conversions.country_code exists in ref_countries when present",
                        """
                            SELECT COUNT(*)
                            FROM lead_conversions lc
                            LEFT JOIN ref_countries rc ON lc.country_code = rc.code
                            WHERE lc.country_code IS NOT NULL
                              AND rc.code IS NULL
                            """,
                    ),
                    (
                        "lead_conversions.converted_at is not null",
                        """
                            SELECT COUNT(*)
                            FROM lead_conversions
                            WHERE converted_at IS NULL
                            """,
                    ),
                ]
            )

        results: list[tuple[str, int, bool]] = []
        for check_name, sql in checks:
            cur.execute(sql)
            issue_count = cur.fetchone()[0]
            ok = issue_count == 0
            results.append((check_name, issue_count, ok))

        checks_table = Table(title="Source DB Checks", show_header=True, header_style="bold cyan")
        checks_table.add_column("Status", width=8)
        checks_table.add_column("Check")
        checks_table.add_column("Issues", justify="right")
        for check_name, issue_count, ok in results:
            status = "[green]PASS[/green]" if ok else "[red]FAIL[/red]"
            checks_table.add_row(status, check_name, str(issue_count))
        console.print(checks_table)

        passed = sum(1 for _, _, ok in results if ok)
        failed = len(results) - passed

        summary_style = "green" if failed == 0 else "red"
        console.print(f"Source DB checks summary: [green]PASS={passed}[/green] [{summary_style}]FAIL={failed}[/{summary_style}]")


def validate_company_distribution() -> None:
    with _db_cursor() as cur:
        cur.execute("SELECT country_code FROM companies")
        country_codes = [row[0] for row in cur.fetchall()]

    _print_top_country_share("Companies", country_codes)


def validate_marketing_leads() -> None:
    latest = _latest_path(storage.get_marketing_leads_path("b2b_leads_*.csv"))
    if not latest:
        console.print("[yellow]Marketing leads: no files found[/yellow]")
        return

    with storage.open(latest, mode="rb") as f:
        leads_df = pl.read_csv(f)

    if "status" in leads_df.columns:
        status_df = leads_df.with_columns(
            pl.col("status")
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strip_chars()
            .alias("_status_norm")
        )
        invalid_status_count = status_df.filter(~pl.col("_status_norm").is_in(sorted(ALLOWED_LEAD_STATUSES))).height
        status_tone = "green" if invalid_status_count == 0 else "red"
        console.print(
            f"Marketing leads status domain: [{status_tone}]invalid={invalid_status_count}[/{status_tone}] "
            f"(allowed={sorted(ALLOWED_LEAD_STATUSES)})"
        )

    lead_countries = leads_df.get_column("country_code").to_list()
    _print_top_country_share("Marketing leads", lead_countries)

    with _db_cursor() as cur:
        cur.execute("SELECT name, country_code FROM companies WHERE type = 'Client'")
        company_rows = [{"company_name": row[0], "company_country_code": row[1]} for row in cur.fetchall()]

    if not company_rows:
        console.print("[yellow]Marketing leads alignment: no client companies found[/yellow]")
        return

    company_df = pl.DataFrame(company_rows)
    existing_df = leads_df.filter(pl.col("is_prospect") == False)  # noqa: E712
    joined = existing_df.join(company_df, on="company_name", how="left")
    comparable = joined.filter(pl.col("company_country_code").is_not_null())
    if comparable.is_empty():
        console.print("[yellow]Marketing leads alignment: no comparable existing-company rows[/yellow]")
        return

    aligned = comparable.filter(pl.col("country_code") == pl.col("company_country_code")).height
    alignment_ratio = aligned / comparable.height
    tone = "green" if alignment_ratio >= 0.9 else "yellow"
    console.print(
        f"Marketing leads alignment (existing-company leads): "
        f"[{tone}]{alignment_ratio:.2%}[/{tone}] ({aligned}/{comparable.height})"
    )


def validate_web_logs(max_rows: int = 20000) -> None:
    latest_daily = _latest_path(storage.get_webserver_logs_path(False, "*.jsonl"))
    latest_seed = _latest_path(storage.get_webserver_logs_path(True, "*.jsonl"))
    latest = latest_daily or latest_seed
    if not latest:
        console.print("[yellow]Web logs: no files found[/yellow]")
        return

    records = []
    with storage.open(latest, mode="rb") as f:
        for idx, line in enumerate(f):
            if idx >= max_rows:
                break
            try:
                obj = json.loads(line.decode("utf-8"))
            except Exception:
                continue
            records.append(
                {
                    "username": obj.get("username"),
                    "country_code": obj.get("country_code"),
                }
            )

    if not records:
        console.print("[yellow]Web logs: no parsable rows[/yellow]")
        return

    logs_df = pl.DataFrame(records)
    _print_top_country_share("Web logs", logs_df.get_column("country_code").to_list())

    auth_df = logs_df.filter(pl.col("username") != "-")
    total_rows = logs_df.height
    auth_rows = auth_df.height
    unauth_rows = total_rows - auth_rows
    auth_ratio = auth_rows / total_rows if total_rows else 0.0
    unauth_ratio = unauth_rows / total_rows if total_rows else 0.0
    console.print(
        f"Web logs auth mix: authenticated=[cyan]{auth_rows}/{total_rows} ({auth_ratio:.2%})[/cyan], "
        f"unauthenticated=[cyan]{unauth_rows}/{total_rows} ({unauth_ratio:.2%})[/cyan]"
    )
    if auth_df.is_empty():
        console.print("[yellow]Web logs alignment: no authenticated rows[/yellow]")
        return

    with _db_cursor() as cur:
        cur.execute(
            """
            SELECT cust.username, comp.country_code
            FROM customers AS cust
            LEFT JOIN companies AS comp
              ON cust.company_cuit = comp.cuit
            """
        )
        user_rows = [{"username": row[0], "user_country_code": row[1]} for row in cur.fetchall()]

    user_df = pl.DataFrame(user_rows)
    joined = auth_df.join(user_df, on="username", how="left")
    comparable = joined.filter(pl.col("user_country_code").is_not_null())
    if comparable.is_empty():
        console.print("[yellow]Web logs alignment: no comparable authenticated rows[/yellow]")
        return

    aligned = comparable.filter(pl.col("country_code") == pl.col("user_country_code")).height
    alignment_ratio = aligned / comparable.height
    tone = "green" if alignment_ratio >= 0.9 else "yellow"
    console.print(
        f"Web logs alignment (authenticated rows only): "
        f"[{tone}]{alignment_ratio:.2%}[/{tone}] ({aligned}/{comparable.height})"
    )


def main(max_web_log_rows: int = 20000) -> None:
    validate_source_db()
    console.rule("[bold]Geography Validation[/bold]")
    validate_company_distribution()
    validate_marketing_leads()
    validate_web_logs(max_rows=max_web_log_rows)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate source DB integrity and geography consistency.")
    parser.add_argument(
        "--max-web-log-rows",
        type=int,
        default=20000,
        help="Maximum number of web-log rows to scan from the latest JSONL file (default: 20000).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.max_web_log_rows < 1:
        raise ValueError("--max-web-log-rows must be >= 1")
    main(max_web_log_rows=args.max_web_log_rows)
