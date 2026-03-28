from functools import cache
from io import BytesIO

import numpy as np
import polars as pl
import pycountry
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.timer import timed_run
from faker import Faker
from pydantic import BaseModel, Field

from b2b_ec_sources import get_connection


# --- CONFIGURATION CLASS ---
class GenConfig(BaseModel):
    # Seed Quantities
    seed_total_companies: int = 100
    seed_supplier_ratio: float = Field(0.20, ge=0.0, le=1.0)
    seed_total_customers: int = 500
    seed_total_products: int = 1000
    seed_total_orders: int = 100000
    catalog_size_seed: int = 100

    # Evolution Probabilities (Now used per 'loop' iteration)
    prob_new_company: float = Field(0.15, ge=0.0, le=1.0)
    prob_price_update: float = Field(0.20, ge=0.0, le=1.0)

    # Organic Growth Rates (Target % increase per run)
    target_cust_growth: float = Field(0.05, ge=0.0, le=1.0)  # 5% new customers
    target_prod_growth: float = Field(0.02, ge=0.0, le=1.0)  # 2% new products for suppliers

    # Evolution Parameters
    catalog_size_evo: int = 20
    min_markup_percent: float = 1.20
    max_markup_percent: float = 1.80
    min_inc_orders: int = 100
    max_inc_orders: int = 2000
    num_event_slots: int = 3


# Instantiating Config
cfg = GenConfig()
fake = Faker()
logger = get_logger("SourceDBGeneration")


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
    logger.info("SCHEMA: Creating B2B2C Schema...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ref_countries (code TEXT PRIMARY KEY, name TEXT NOT NULL);
        
        CREATE TABLE IF NOT EXISTS companies (
            cuit TEXT PRIMARY KEY, 
            name TEXT NOT NULL, 
            type TEXT CHECK (type IN ('Supplier', 'Client')), 
            country_code TEXT REFERENCES ref_countries(code),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY, 
            name TEXT NOT NULL, 
            supplier_cuit TEXT REFERENCES companies(cuit), 
            base_price DECIMAL(12,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS company_catalogs (
            company_cuit TEXT REFERENCES companies(cuit),
            product_id INTEGER REFERENCES products(id),
            sale_price DECIMAL(12,2),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (company_cuit, product_id)
        );

        CREATE TABLE IF NOT EXISTS customers (
            id SERIAL PRIMARY KEY, 
            company_cuit TEXT REFERENCES companies(cuit), 
            document_number TEXT UNIQUE NOT NULL,
            username TEXT UNIQUE NOT NULL,
            first_name TEXT NOT NULL, last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL, birth_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY, 
            company_cuit TEXT REFERENCES companies(cuit), 
            customer_id INTEGER REFERENCES customers(id),
            order_date TIMESTAMP, total_amount DECIMAL(15,2),
            created_at TIMESTAMP DEFAULT (now() at time zone 'utc')
        );

        CREATE TABLE IF NOT EXISTS order_items (
            id SERIAL PRIMARY KEY, 
            order_id INTEGER REFERENCES orders(id), 
            product_id INTEGER, quantity INTEGER, unit_price DECIMAL(12,2)
        );
    """)


@cache
def get_iso_data():
    return [{"code": c.alpha_2, "name": getattr(c, "common_name", c.name)} for c in pycountry.countries]


def generate_customer_data(company_cuit):
    fn, ln = fake.first_name(), fake.last_name()
    username = f"{fn[0].lower()}_{ln.lower()}_{fake.bothify('###')}"
    email = f"{username}@{fake.free_email_domain()}"
    return {
        "company_cuit": company_cuit,
        "document_number": fake.unique.bothify("########"),
        "first_name": fn,
        "last_name": ln,
        "username": username,
        "email": email,
        "birth_date": fake.date_of_birth(minimum_age=21, maximum_age=70),
    }


def generate_batch(conn, num_orders=1000, is_seed=False):
    stats = {"co": 0, "cust": 0, "prod": 0, "ord": 0, "items": 0}

    if not is_seed:
        # --- 1. ORGANIC GROWTH: EXISTING ENTITIES ---
        with conn.cursor() as cur:
            # Add New Customers to existing Clients
            cur.execute("SELECT count(*) FROM customers")
            curr_cust = cur.fetchone()[0]
            num_new_custs = max(1, int(curr_cust * cfg.target_cust_growth))

            cur.execute("SELECT cuit FROM companies WHERE type = 'Client'")
            client_cuits = [r[0] for r in cur.fetchall()]

            if client_cuits:
                new_cust_df = pl.DataFrame(
                    [generate_customer_data(np.random.choice(client_cuits)) for _ in range(num_new_custs)]
                )
                pg_bulk_copy(conn, new_cust_df, "customers")
                stats["cust"] += num_new_custs

            # Add New Products to existing Suppliers
            cur.execute("SELECT count(*) FROM products")
            curr_prod = cur.fetchone()[0]
            num_new_prods = max(1, int(curr_prod * cfg.target_prod_growth))

            cur.execute("SELECT cuit FROM companies WHERE type = 'Supplier'")
            sup_cuits = [r[0] for r in cur.fetchall()]

            if sup_cuits:
                new_prod_df = pl.DataFrame(
                    [
                        {
                            "name": f"Product_Ext_{fake.word()}_{fake.bothify('###')}",
                            "supplier_cuit": np.random.choice(sup_cuits),
                            "base_price": round(np.random.uniform(10, 500), 2),
                        }
                        for _ in range(num_new_prods)
                    ]
                )
                pg_bulk_copy(conn, new_prod_df, "products")
                stats["prod"] += num_new_prods

        # --- 2. EVOLUTION LOOP: RANDOM EVENTS ---
        # We run some 'event slots' per batch execution
        for _ in range(cfg.num_event_slots):
            # Chance for a brand new Company
            if np.random.random() <= cfg.prob_new_company:
                is_sup = np.random.random() < 0.5
                new_cuit = fake.unique.bothify("##-########-#")
                all_iso = [c["code"] for c in get_iso_data()]

                pg_bulk_copy(
                    conn,
                    pl.DataFrame(
                        [
                            {
                                "cuit": new_cuit,
                                "name": fake.company(),
                                "type": "Supplier" if is_sup else "Client",
                                "country_code": np.random.choice(all_iso),
                            }
                        ]
                    ),
                    "companies",
                )
                stats["co"] += 1

                if not is_sup:  # If it's a client, give them a catalog immediately
                    with conn.cursor() as cur:
                        cur.execute(
                            f"SELECT id, base_price FROM products ORDER BY RANDOM() LIMIT {cfg.catalog_size_evo}"
                        )
                        p_list = cur.fetchall()
                        pg_bulk_copy(
                            conn,
                            pl.DataFrame(
                                [
                                    {
                                        "company_cuit": new_cuit,
                                        "product_id": p[0],
                                        "sale_price": np.round(
                                            float(p[1])
                                            * np.random.uniform(cfg.min_markup_percent, cfg.max_markup_percent),
                                            2,
                                        ),
                                    }
                                    for p in p_list
                                ]
                            ),
                            "company_catalogs",
                        )

            # Chance for a Price Update
            if np.random.random() < cfg.prob_price_update:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE company_catalogs "
                        "SET sale_price = sale_price * (0.95 + (random() * 0.1)), updated_at = now() "
                        "WHERE company_cuit IN (SELECT company_cuit FROM companies WHERE type = 'Client' ORDER BY random() LIMIT 1)"
                    )

    # --- 3. ORDER GENERATION (Optimized) ---
    with conn.cursor() as cur:
        cur.execute("SELECT product_id, company_cuit, sale_price FROM company_catalogs")
        catalogs = {}
        for pid, cuit, price in cur.fetchall():
            catalogs.setdefault(cuit, []).append((pid, float(price)))

        cur.execute("SELECT id, company_cuit FROM customers")
        cust_map = {r[0]: r[1] for r in cur.fetchall()}

    order_list, items_by_order = [], []
    c_ids = list(cust_map.keys())

    if not c_ids:
        logger.warning("No customers available for orders.")
        return

    for _ in range(num_orders):
        cid = int(np.random.choice(c_ids))
        cuit = cust_map[cid]
        cat = catalogs.get(cuit, [])
        if not cat:
            continue

        o_date = fake.date_time_between(start_date="-1y" if is_seed else "-1d", end_date="now")
        num_items = np.random.randint(1, 6)
        total, current_items = 0, []

        selected = np.random.choice(len(cat), size=min(num_items, len(cat)), replace=False)
        for idx in selected:
            pid, price = cat[idx]
            qty = np.random.randint(1, 5)
            total += qty * price
            current_items.append({"product_id": pid, "quantity": qty, "unit_price": price})

        order_list.append(
            {"company_cuit": cuit, "customer_id": cid, "order_date": o_date, "total_amount": round(total, 2)}
        )
        items_by_order.append(current_items)

    pg_bulk_copy(conn, pl.DataFrame(order_list), "orders")

    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM orders ORDER BY id DESC LIMIT {len(order_list)}")
        new_ids = [r[0] for r in cur.fetchall()][::-1]

    final_items = []
    for idx, oid in enumerate(new_ids):
        for item in items_by_order[idx]:
            item["order_id"] = oid
            final_items.append(item)

    pg_bulk_copy(conn, pl.DataFrame(final_items), "order_items")

    logger.info(
        f"BATCH SUMMARY: +{stats['co']} Co, +{stats['cust']} Cust, +{stats['prod']} Prod, +{len(order_list)} Orders (+{len(final_items)} Items)"
    )


@timed_run
def run_source_generation():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'orders')")
        initialized = cur.fetchone()[0]

    if not initialized:
        logger.info(f"SEED: Starting B2B2C {cfg.seed_total_orders} Seed...")
        with conn.cursor() as cur:
            create_schema(cur)
        conn.commit()

        iso = get_iso_data()
        pg_bulk_copy(conn, pl.DataFrame(iso), "ref_countries")
        all_iso = [c["code"] for c in iso]

        # Seed Companies
        num_sup = int(cfg.seed_total_companies * cfg.seed_supplier_ratio)
        cos = [
            {
                "cuit": fake.unique.bothify("##-########-#"),
                "name": fake.company(),
                "type": "Supplier" if i < num_sup else "Client",
                "country_code": np.random.choice(all_iso),
            }
            for i in range(cfg.seed_total_companies)
        ]
        pg_bulk_copy(conn, pl.DataFrame(cos), "companies")

        # Seed Products
        sup_cuits = [c["cuit"] for c in cos if c["type"] == "Supplier"]
        prods = [
            {
                "name": f"Product_{i}",
                "supplier_cuit": np.random.choice(sup_cuits),
                "base_price": round(np.random.uniform(10, 500), 2),
            }
            for i in range(cfg.seed_total_products)
        ]
        pg_bulk_copy(conn, pl.DataFrame(prods), "products")

        with conn.cursor() as cur:
            cur.execute("SELECT id, base_price FROM products")
            all_p = cur.fetchall()
            client_cuits = [c["cuit"] for c in cos if c["type"] == "Client"]
            cats = []
            for cuit in client_cuits:
                for idx in np.random.choice(len(all_p), cfg.catalog_size_seed, replace=False):
                    pid, bp = all_p[idx]
                    cats.append(
                        {
                            "company_cuit": cuit,
                            "product_id": pid,
                            "sale_price": np.round(
                                float(bp) * np.random.uniform(cfg.min_markup_percent, cfg.max_markup_percent), 2
                            ),
                        }
                    )
            pg_bulk_copy(conn, pl.DataFrame(cats), "company_catalogs")
            pg_bulk_copy(
                conn,
                pl.DataFrame(
                    [generate_customer_data(np.random.choice(client_cuits)) for _ in range(cfg.seed_total_customers)]
                ),
                "customers",
            )

        generate_batch(conn, num_orders=cfg.seed_total_orders, is_seed=True)
    else:
        generate_batch(conn, num_orders=np.random.randint(cfg.min_inc_orders, cfg.max_inc_orders), is_seed=False)
    conn.close()


if __name__ == "__main__":
    run_source_generation()
