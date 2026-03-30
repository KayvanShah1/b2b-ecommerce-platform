import random
from datetime import datetime, timedelta
from io import BytesIO

import numpy as np
import polars as pl
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.timer import timed_run
from faker import Faker
from pydantic import BaseModel

from b2b_ec_sources import get_connection, get_iso_data
from b2b_ec_sources.temporal_sampling import (
    build_month_probability_vector,
    ordered_bounds,
    sample_seasonal_volume,
    sample_timestamp_within_window,
)


# --- CONFIGURATION ---
class GeneratorParameters(BaseModel):
    # Initial Seed Volume
    seed_total_companies: int = 100
    seed_supplier_ratio: float = 0.20
    seed_total_customers: int = 10000
    seed_total_products: int = 700
    seed_total_orders: int = 100000
    catalog_size_seed: int = 250

    # Organic Growth (Steady State)
    min_target_cust_growth: float = 0.01
    max_target_cust_growth: float = 0.10
    min_target_prod_growth: float = 0.01
    max_target_prod_growth: float = 0.05

    # Dynamic Evolution Engine
    min_event_slots: int = 1  # Minimum event attempts per run
    max_event_slots: int = 5  # Maximum event attempts per run
    prob_new_company: float = 0.15  # Chance per slot for a new Supplier/Client
    prob_price_update: float = 0.05  # Chance per slot for a Supplier price shift
    prob_supplier: float = 0.20  # If new company, chance it's a Supplier vs Client
    prob_client: float = 0.80  # If new company, chance it's a Supplier vs Client

    # Financials
    min_markup_percent: float = 1.20
    max_markup_percent: float = 2.50
    min_base_price: float = 10.0
    max_base_price: float = 1000.0

    # Incremental (daily) order volume targets in steady state
    min_inc_orders: int = 100
    max_inc_orders: int = 450
    min_items_per_order: int = 1
    max_items_per_order: int = 6
    min_item_quantity: int = 1
    max_item_quantity: int = 5
    new_supplier_products: int = 20

    # Statuses & Returns
    seed_status_weights: list = [0.93, 0.03, 0.04]  # COMPLETED, CANCELLED, RETURNED
    min_return_rate: float = 0.02
    max_return_rate: float = 0.06

    # Temporal Order Distribution (Monthly + Seasonality + Skew + Jitter)
    seed_distribution_days: int = 365
    incremental_distribution_days: int = 60
    base_month_weights: list[float] = [1.0] * 12  # Jan..Dec baseline preference
    seasonality_amplitude: float = 0.35  # 0.0-0.95, scales cosine seasonality curve
    seasonality_peak_month: int = 11  # 1..12, month with highest seasonal lift
    month_jitter_sigma: float = 0.20  # multiplicative month-to-month volatility
    incremental_volume_jitter_sigma: float = 0.08  # daily volume volatility around seasonal target
    min_incremental_volume_factor: float = 0.60  # clamp factor against min_inc_orders
    max_incremental_volume_factor: float = 1.60  # clamp factor against max_inc_orders
    intra_month_skew_alpha: float = 2.2  # Beta(alpha, beta) left/right month skew
    intra_month_skew_beta: float = 2.8
    day_jitter_std: float = 1.5  # additive noise in days
    hour_jitter_std: float = 3.0  # additive noise in hours


cfg = GeneratorParameters()
fake = Faker()
logger = get_logger("SourceDBGeneration")


# --- UTILS ---
def pg_bulk_copy(conn, df: pl.DataFrame, table_name: str):
    if df.is_empty():
        return
    buffer = BytesIO()
    df.write_csv(buffer, include_header=False, separator="|")
    buffer.seek(0)
    with conn.cursor() as cursor:
        cursor.copy_from(buffer, table_name, sep="|", columns=df.columns)
    conn.commit()


def create_schema(cur):
    logger.info("SCHEMA: Initializing Relational Tables with Audit Columns...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ref_countries (code TEXT PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS companies (
            cuit TEXT PRIMARY KEY, 
            name TEXT NOT NULL, 
            type TEXT CHECK (type IN ('Supplier', 'Client')), 
            country_code TEXT REFERENCES ref_countries(code), 
            created_at TIMESTAMP DEFAULT (now() at time zone 'utc'), 
            updated_at TIMESTAMP DEFAULT (now() at time zone 'utc')
        );
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY, 
            name TEXT NOT NULL, 
            supplier_cuit TEXT REFERENCES companies(cuit), 
            base_price DECIMAL(12,2), 
            created_at TIMESTAMP DEFAULT (now() at time zone 'utc'),
            updated_at TIMESTAMP DEFAULT (now() at time zone 'utc')
        );
        CREATE TABLE IF NOT EXISTS company_catalogs (
            company_cuit TEXT REFERENCES companies(cuit), 
            product_id INTEGER REFERENCES products(id),
            sale_price DECIMAL(12,2), 
            created_at TIMESTAMP DEFAULT (now() at time zone 'utc'),
            updated_at TIMESTAMP DEFAULT (now() at time zone 'utc'), 
            PRIMARY KEY (company_cuit, product_id)
        );
        CREATE TABLE IF NOT EXISTS customers (
            id SERIAL PRIMARY KEY, 
            company_cuit TEXT REFERENCES companies(cuit), 
            document_number TEXT UNIQUE NOT NULL,
            username TEXT UNIQUE NOT NULL, 
            first_name TEXT NOT NULL, 
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone_number TEXT,
            birth_date DATE NOT NULL, 
            created_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY, 
            company_cuit TEXT REFERENCES companies(cuit), 
            customer_id INTEGER REFERENCES customers(id),
            status TEXT CHECK (status IN ('COMPLETED', 'CANCELLED', 'RETURNED')), 
            order_date TIMESTAMP, 
            total_amount DECIMAL(15,2),
            created_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT (now() at time zone 'utc')
        );
        CREATE TABLE IF NOT EXISTS order_items (
            id SERIAL PRIMARY KEY, 
            order_id INTEGER REFERENCES orders(id), 
            product_id INTEGER, 
            quantity INTEGER, unit_price DECIMAL(12,2)
        );
    """)


def generate_customer_data(company_cuit, backdate_from="-1y"):
    fn, ln = fake.first_name(), fake.last_name()
    username = fake.unique.bothify(f"{fn[:1].lower()}_{ln.lower()}_#####")
    return {
        "company_cuit": company_cuit,
        "document_number": fake.unique.bothify("########"),
        "username": username,
        "first_name": fn,
        "last_name": ln,
        "email": f"{username}@{fake.free_email_domain()}",
        "phone_number": fake.phone_number(),
        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=80),
        "created_at": fake.date_time_between(start_date=backdate_from, end_date="now"),
    }


def _sample_base_price() -> float:
    min_price, max_price = ordered_bounds(float(cfg.min_base_price), float(cfg.max_base_price))
    return round(random.uniform(min_price, max_price), 2)


def _sample_items_per_order() -> int:
    min_items, max_items = ordered_bounds(int(cfg.min_items_per_order), int(cfg.max_items_per_order))
    return random.randint(min_items, max_items)


def _sample_item_quantity() -> int:
    min_qty, max_qty = ordered_bounds(int(cfg.min_item_quantity), int(cfg.max_item_quantity))
    return random.randint(min_qty, max_qty)


def _build_calendar_month_probabilities() -> np.ndarray:
    return build_month_probability_vector(
        base_month_weights=cfg.base_month_weights,
        seasonality_amplitude=cfg.seasonality_amplitude,
        seasonality_peak_month=cfg.seasonality_peak_month,
        month_jitter_sigma=cfg.month_jitter_sigma,
    )


def _sample_incremental_order_volume(now_ts: datetime) -> int:
    return sample_seasonal_volume(
        min_count=cfg.min_inc_orders,
        max_count=cfg.max_inc_orders,
        now_ts=now_ts,
        base_month_weights=cfg.base_month_weights,
        seasonality_amplitude=cfg.seasonality_amplitude,
        seasonality_peak_month=cfg.seasonality_peak_month,
        volume_jitter_sigma=cfg.incremental_volume_jitter_sigma,
        min_factor=cfg.min_incremental_volume_factor,
        max_factor=cfg.max_incremental_volume_factor,
    )


def _get_distribution_window_start(now_ts: datetime, is_seed: bool) -> datetime:
    window_days = cfg.seed_distribution_days if is_seed else cfg.incremental_distribution_days
    return now_ts - timedelta(days=max(1, int(window_days)))


def _sample_order_date_with_temporal_pattern(
    customer_created_at: datetime | None, now_ts: datetime, month_probs: np.ndarray, is_seed: bool
) -> datetime:
    window_start = _get_distribution_window_start(now_ts, is_seed)
    if customer_created_at is None:
        customer_created_at = window_start
    effective_start = max(customer_created_at, window_start)
    if effective_start >= now_ts:
        return now_ts
    return sample_timestamp_within_window(
        window_start=effective_start,
        window_end=now_ts,
        month_probs=month_probs,
        intra_month_skew_alpha=cfg.intra_month_skew_alpha,
        intra_month_skew_beta=cfg.intra_month_skew_beta,
        day_jitter_std=cfg.day_jitter_std,
        hour_jitter_std=cfg.hour_jitter_std,
        clamp_jitter_to_bucket=False,
    )


# --- BATCH GENERATOR ---
def generate_batch(conn, num_orders=1000, is_seed=False):
    stats = {"cust": 0, "prod": 0, "returns": 0, "new_co": 0, "price_upd": 0}

    with conn.cursor() as cur:
        if not is_seed:
            cur.execute("SELECT cuit, type FROM companies")
            all_cos = cur.fetchall()
            client_cuits = [c[0] for c in all_cos if c[1] == "Client"]
            sup_cuits = [c[0] for c in all_cos if c[1] == "Supplier"]

            # 1. DYNAMIC EVENT SLOTS (Multi-Dice Rolls)
            num_slots = random.randint(cfg.min_event_slots, cfg.max_event_slots)
            for _ in range(num_slots):
                # Chance for New Company (Supplier or Client)
                if random.random() < cfg.prob_new_company:
                    new_cuit = fake.unique.bothify("##-########-#")
                    co_type = random.choices(["Supplier", "Client"], weights=[cfg.prob_supplier, cfg.prob_client])[0]
                    now_ts = datetime.now()
                    cur.execute(
                        "INSERT INTO companies (cuit, name, type, country_code, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
                        (
                            new_cuit,
                            fake.company(),
                            co_type,
                            random.choice([c["code"] for c in get_iso_data()]),
                            now_ts,
                            now_ts,
                        ),
                    )

                    if co_type == "Client":
                        cur.execute(
                            "SELECT id, base_price FROM products ORDER BY RANDOM() LIMIT %s", (cfg.catalog_size_seed,)
                        )
                        p_list = cur.fetchall()
                        pg_bulk_copy(
                            conn,
                            pl.DataFrame(
                                [
                                    {
                                        "company_cuit": new_cuit,
                                        "product_id": p[0],
                                        "sale_price": round(float(p[1]) * 1.5, 2),
                                        "created_at": now_ts,
                                        "updated_at": now_ts,  # Initialize audit on catalog entry
                                    }
                                    for p in p_list
                                ]
                            ),
                            "company_catalogs",
                        )
                        client_cuits.append(new_cuit)
                    else:
                        pg_bulk_copy(
                            conn,
                            pl.DataFrame(
                                [
                                    {
                                        "name": f"SKU_{fake.word()}",
                                        "supplier_cuit": new_cuit,
                                        "base_price": _sample_base_price(),
                                        "created_at": now_ts,
                                        "updated_at": now_ts,
                                    }
                                    for _ in range(max(1, int(cfg.new_supplier_products)))
                                ]
                            ),
                            "products",
                        )
                        sup_cuits.append(new_cuit)
                    stats["new_co"] += 1

                # Chance for Price Shift (Cascading Audit)
                if random.random() < cfg.prob_price_update:
                    target_sup = random.choice(sup_cuits)
                    # Update Wholesale Source + Audit
                    cur.execute(
                        "UPDATE products SET base_price = base_price * %s, updated_at = NOW() WHERE supplier_cuit = %s",
                        (random.uniform(0.95, 1.10), target_sup),
                    )
                    # Update Retail Catalog Contract + Audit
                    cur.execute(
                        """
                        UPDATE company_catalogs cc 
                        SET sale_price = p.base_price * 1.4, updated_at = NOW()
                        FROM products p WHERE cc.product_id = p.id AND p.supplier_cuit = %s
                        """,
                        (target_sup,),
                    )
                    stats["price_upd"] += 1

            # 2. STEADY ORGANIC GROWTH
            num_new_custs = max(
                1, int(len(client_cuits) * np.random.uniform(cfg.min_target_cust_growth, cfg.max_target_cust_growth))
            )
            pg_bulk_copy(
                conn,
                pl.DataFrame(
                    [generate_customer_data(random.choice(client_cuits), "-1d") for _ in range(num_new_custs)]
                ),
                "customers",
            )

            num_new_prods = max(
                1,
                int(
                    cfg.seed_total_products * np.random.uniform(cfg.min_target_prod_growth, cfg.max_target_prod_growth)
                ),
            )
            now_ts = datetime.now()
            pg_bulk_copy(
                conn,
                pl.DataFrame(
                    [
                        {
                            "name": f"Prod_{fake.word()}",
                            "supplier_cuit": random.choice(sup_cuits),
                            "base_price": _sample_base_price(),
                            "created_at": now_ts,
                            "updated_at": now_ts,
                        }
                        for _ in range(num_new_prods)
                    ]
                ),
                "products",
            )
            stats.update({"cust": num_new_custs, "prod": num_new_prods})

            # 3. RETROACTIVE LOGISTICS (Returns 3-7 day window)
            window_start, window_end = datetime.now() - timedelta(days=7), datetime.now() - timedelta(days=3)
            cur.execute(
                "SELECT id FROM orders WHERE status = 'COMPLETED' AND order_date BETWEEN %s AND %s",
                (window_start, window_end),
            )
            eligible = [r[0] for r in cur.fetchall()]
            if eligible:
                to_ret = random.sample(
                    eligible, max(1, int(len(eligible) * random.uniform(cfg.min_return_rate, cfg.max_return_rate)))
                )
                # FIX: Set updated_at when status changes to RETURNED
                cur.execute("UPDATE orders SET status = 'RETURNED', updated_at = NOW() WHERE id = ANY(%s)", (to_ret,))
                stats["returns"] = len(to_ret)

        # STATE RECOVERY
        cur.execute("SELECT product_id, company_cuit, sale_price FROM company_catalogs")
        catalogs = {}
        for pid, cuit, price in cur.fetchall():
            catalogs.setdefault(cuit, []).append((pid, float(price)))
        cur.execute("SELECT id, company_cuit, created_at FROM customers")
        cust_info = {r[0]: {"cuit": r[1], "created_at": r[2]} for r in cur.fetchall()}

    # 4. TRANSACTIONAL GENERATION
    order_list, items_by_order = [], []
    now_ts = datetime.now()
    calendar_month_probs = _build_calendar_month_probabilities()
    valid_customers = []
    for cid, meta in cust_info.items():
        cat = catalogs.get(meta["cuit"], [])
        if not cat:
            continue
        created_at = meta["created_at"] if meta["created_at"] is not None else datetime.min
        valid_customers.append({"id": cid, "cuit": meta["cuit"], "created_at": created_at})

    if not valid_customers:
        logger.warning("BATCH: No eligible customers with catalogs found. Skipping order generation.")
        return

    valid_customers.sort(key=lambda x: x["created_at"])
    sampled_order_dates = [
        _sample_order_date_with_temporal_pattern(
            customer_created_at=None,
            now_ts=now_ts,
            month_probs=calendar_month_probs,
            is_seed=is_seed,
        )
        for _ in range(num_orders)
    ]
    sampled_order_dates.sort()

    customer_cursor = 0
    eligible_customers = []
    for o_date in sampled_order_dates:
        while customer_cursor < len(valid_customers) and valid_customers[customer_cursor]["created_at"] <= o_date:
            eligible_customers.append(valid_customers[customer_cursor])
            customer_cursor += 1
        if not eligible_customers:
            continue

        chosen_customer = random.choice(eligible_customers)
        cid = chosen_customer["id"]
        customer_cuit = chosen_customer["cuit"]
        cat = catalogs[customer_cuit]

        status = random.choices(
            ["COMPLETED", "CANCELLED", "RETURNED" if is_seed else "COMPLETED"],
            weights=cfg.seed_status_weights if is_seed else [0.97, 0.03, 0],
        )[0]

        num_items = _sample_items_per_order()
        selected = np.random.choice(len(cat), size=min(num_items, len(cat)), replace=False)
        total, current_items = 0, []
        for idx in selected:
            pid, price = cat[idx]
            qty = _sample_item_quantity()
            total += qty * price
            current_items.append({"product_id": pid, "quantity": qty, "unit_price": price})

        order_list.append(
            {
                "company_cuit": customer_cuit,
                "customer_id": cid,
                "status": status,
                "order_date": o_date,
                "total_amount": round(total, 2),
                "created_at": o_date,  # FIX: created_at matches order_date
                "updated_at": o_date,  # Initial updated_at matches created_at
            }
        )
        items_by_order.append(current_items)

    if len(order_list) < num_orders:
        logger.warning(
            f"BATCH: Generated {len(order_list)} of {num_orders} requested orders due to customer eligibility by date."
        )

    pg_bulk_copy(conn, pl.DataFrame(order_list), "orders")

    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM orders ORDER BY id DESC LIMIT {len(order_list)}")
        new_ids = [r[0] for r in cur.fetchall()][::-1]

    final_items = []
    for i, oid in enumerate(new_ids):
        for item in items_by_order[i]:
            item["order_id"] = oid
            final_items.append(item)
    pg_bulk_copy(conn, pl.DataFrame(final_items), "order_items")

    logger.info(
        f"BATCH SUMMARY: +{stats['new_co']} Co, +{stats['cust']} Cust, "
        f"+{stats['prod']} Prod, +{stats['price_upd']} PriceShift, "
        f"+{len(order_list)} Orders (+{len(final_items)} Items)"
    )


# --- MAIN RUNNER ---
@timed_run
def run_source_generation():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'orders')")
        initialized = cur.fetchone()[0]

    if not initialized:
        logger.info("SEED: Generating 12-Month Relational Baseline...")
        with conn.cursor() as cur:
            create_schema(cur)
        iso = get_iso_data()
        pg_bulk_copy(conn, pl.DataFrame(iso), "ref_countries")

        num_sup = int(cfg.seed_total_companies * cfg.seed_supplier_ratio)
        cos = []
        for i in range(cfg.seed_total_companies):
            ts = fake.date_time_between("-2y", "-1y")
            cos.append(
                {
                    "cuit": fake.unique.bothify("##-########-#"),
                    "name": fake.company(),
                    "type": "Supplier" if i < num_sup else "Client",
                    "country_code": random.choice([c["code"] for c in iso]),
                    "created_at": ts,
                    "updated_at": ts,  # FIX: initialized updated_at
                }
            )
        pg_bulk_copy(conn, pl.DataFrame(cos), "companies")

        sup_cuits = [c["cuit"] for c in cos if c["type"] == "Supplier"]
        prods = []
        for i in range(cfg.seed_total_products):
            ts = fake.date_time_between("-1y", "-10m")
            prods.append(
                {
                    "name": f"P_{i}",
                    "supplier_cuit": random.choice(sup_cuits),
                    "base_price": _sample_base_price(),
                    "created_at": ts,
                    "updated_at": ts,
                }
            )
        pg_bulk_copy(conn, pl.DataFrame(prods), "products")

        with conn.cursor() as cur:
            cur.execute("SELECT id, base_price, created_at FROM products")
            all_p = cur.fetchall()
            client_cuits = [c["cuit"] for c in cos if c["type"] == "Client"]
            cats = []
            for cuit in client_cuits:
                # Weighted choice for catalog items
                selected = np.random.choice(len(all_p), cfg.catalog_size_seed, replace=False)
                for idx in selected:
                    pid, bp, p_ts = all_p[idx]
                    cats.append(
                        {
                            "company_cuit": cuit,
                            "product_id": pid,
                            "sale_price": round(
                                float(bp) * random.uniform(cfg.min_markup_percent, cfg.max_markup_percent), 2
                            ),
                            "created_at": p_ts,  # Catalog created after product
                            "updated_at": p_ts,
                        }
                    )
            pg_bulk_copy(conn, pl.DataFrame(cats), "company_catalogs")
            pg_bulk_copy(
                conn,
                pl.DataFrame(
                    [generate_customer_data(random.choice(client_cuits)) for _ in range(cfg.seed_total_customers)]
                ),
                "customers",
            )

        generate_batch(conn, num_orders=cfg.seed_total_orders, is_seed=True)
    else:
        logger.info("EVOLUTION: Running Dynamic Market Cycle...")
        inc_orders = _sample_incremental_order_volume(datetime.now())
        logger.info(f"EVOLUTION: Seasonal daily order target = {inc_orders}")
        generate_batch(conn, num_orders=inc_orders, is_seed=False)
    conn.close()


if __name__ == "__main__":
    run_source_generation()
