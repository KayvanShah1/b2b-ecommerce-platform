import json

import polars as pl

from b2b_ec_sources import get_connection
from b2b_ec_sources.geography import GEO
from b2b_ec_utils.storage import storage


def _latest_path(pattern: str) -> str | None:
    files = storage.glob(pattern)
    if not files:
        return None
    return sorted(files)[-1]


def _print_top_country_share(label: str, country_codes: list[str]) -> None:
    if not country_codes:
        print(f"{label}: no rows")
        return

    top_set = {c.upper() for c in GEO.top_country_codes}
    total = len(country_codes)
    top_count = sum(1 for c in country_codes if (c or "").upper() in top_set)
    share = top_count / total
    print(
        f"{label}: top-10 share = {share:.2%} ({top_count}/{total}) "
        f"[target={GEO.top_country_share:.0%}]"
    )


def _print_check(name: str, issue_count: int) -> bool:
    ok = issue_count == 0
    status = "PASS" if ok else "FAIL"
    print(f"{status}: {name} -> issues={issue_count}")
    return ok


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

    print("=== Source DB Validation ===")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            missing_tables: list[str] = []
            for table in required_tables:
                cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
                if cur.fetchone()[0] is None:
                    missing_tables.append(table)

            if missing_tables:
                print(f"FAIL: Missing required tables: {', '.join(missing_tables)}")
                return

            counts = {}
            for table in required_tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cur.fetchone()[0]

            print(
                "Row counts: "
                + ", ".join(f"{table}={counts[table]}" for table in required_tables)
            )

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

            passed = 0
            failed = 0
            for check_name, sql in checks:
                cur.execute(sql)
                issue_count = cur.fetchone()[0]
                if _print_check(check_name, issue_count):
                    passed += 1
                else:
                    failed += 1

            print(f"Source DB checks summary: PASS={passed} FAIL={failed}")
    finally:
        conn.close()


def validate_company_distribution() -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT country_code FROM companies")
            country_codes = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()

    _print_top_country_share("Companies", country_codes)


def validate_marketing_leads() -> None:
    latest = _latest_path(storage.get_marketing_leads_path("b2b_leads_*.csv"))
    if not latest:
        print("Marketing leads: no files found")
        return

    with storage.open(latest, mode="rb") as f:
        leads_df = pl.read_csv(f)

    lead_countries = leads_df.get_column("country_code").to_list()
    _print_top_country_share("Marketing leads", lead_countries)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT name, country_code FROM companies WHERE type = 'Client'")
            company_rows = [{"company_name": row[0], "company_country_code": row[1]} for row in cur.fetchall()]
    finally:
        conn.close()

    if not company_rows:
        print("Marketing leads alignment: no client companies found")
        return

    company_df = pl.DataFrame(company_rows)
    existing_df = leads_df.filter(pl.col("is_prospect") == False)  # noqa: E712
    joined = existing_df.join(company_df, on="company_name", how="left")
    comparable = joined.filter(pl.col("company_country_code").is_not_null())
    if comparable.is_empty():
        print("Marketing leads alignment: no comparable existing-company rows")
        return

    aligned = comparable.filter(pl.col("country_code") == pl.col("company_country_code")).height
    alignment_ratio = aligned / comparable.height
    print(
        f"Marketing leads alignment (existing-company leads): "
        f"{alignment_ratio:.2%} ({aligned}/{comparable.height})"
    )


def validate_web_logs(max_rows: int = 20000) -> None:
    latest_daily = _latest_path(storage.get_webserver_logs_path(False, "*.jsonl"))
    latest_seed = _latest_path(storage.get_webserver_logs_path(True, "*.jsonl"))
    latest = latest_daily or latest_seed
    if not latest:
        print("Web logs: no files found")
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
        print("Web logs: no parsable rows")
        return

    logs_df = pl.DataFrame(records)
    _print_top_country_share("Web logs", logs_df.get_column("country_code").to_list())

    auth_df = logs_df.filter(pl.col("username") != "-")
    total_rows = logs_df.height
    auth_rows = auth_df.height
    unauth_rows = total_rows - auth_rows
    auth_ratio = auth_rows / total_rows if total_rows else 0.0
    unauth_ratio = unauth_rows / total_rows if total_rows else 0.0
    print(
        f"Web logs auth mix: authenticated={auth_rows}/{total_rows} ({auth_ratio:.2%}), "
        f"unauthenticated={unauth_rows}/{total_rows} ({unauth_ratio:.2%})"
    )
    if auth_df.is_empty():
        print("Web logs alignment: no authenticated rows")
        return

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cust.username, comp.country_code
                FROM customers AS cust
                LEFT JOIN companies AS comp
                  ON cust.company_cuit = comp.cuit
                """
            )
            user_rows = [{"username": row[0], "user_country_code": row[1]} for row in cur.fetchall()]
    finally:
        conn.close()

    user_df = pl.DataFrame(user_rows)
    joined = auth_df.join(user_df, on="username", how="left")
    comparable = joined.filter(pl.col("user_country_code").is_not_null())
    if comparable.is_empty():
        print("Web logs alignment: no comparable authenticated rows")
        return

    aligned = comparable.filter(pl.col("country_code") == pl.col("user_country_code")).height
    alignment_ratio = aligned / comparable.height
    print(
        f"Web logs alignment (authenticated rows only): "
        f"{alignment_ratio:.2%} ({aligned}/{comparable.height})"
    )


def main() -> None:
    validate_source_db()
    print("=== Geography Validation ===")
    validate_company_distribution()
    validate_marketing_leads()
    validate_web_logs()


if __name__ == "__main__":
    main()
