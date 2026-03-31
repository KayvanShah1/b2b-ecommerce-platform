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


class CSDGParameters(BaseModel):
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
    min_event_slots: int = 1
    max_event_slots: int = 5
    prob_new_company: float = 0.15
    prob_price_update: float = 0.05
    prob_supplier: float = 0.20
    prob_client: float = 0.80

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
    seed_status_weights: list[float] = [0.93, 0.03, 0.04]  # COMPLETED, CANCELLED, RETURNED
    min_return_rate: float = 0.02
    max_return_rate: float = 0.06

    # Temporal Order Distribution (Monthly + Seasonality + Skew + Jitter)
    seed_distribution_days: int = 365
    incremental_distribution_days: int = 60
    base_month_weights: list[float] = [1.0] * 12
    seasonality_amplitude: float = 0.35
    seasonality_peak_month: int = 11
    month_jitter_sigma: float = 0.20
    incremental_volume_jitter_sigma: float = 0.08
    min_incremental_volume_factor: float = 0.60
    max_incremental_volume_factor: float = 1.60
    intra_month_skew_alpha: float = 2.2
    intra_month_skew_beta: float = 2.8
    day_jitter_std: float = 1.5
    hour_jitter_std: float = 3.0


DEFAULT_CSDG_PARAMS = CSDGParameters()
DEFAULT_FAKE = Faker()
DEFAULT_LOGGER = get_logger("SourceDBGeneration")


class CommerceSourceDataGenerator:
    def __init__(self, params: CSDGParameters | None = None):
        self.params = params or DEFAULT_CSDG_PARAMS
        self.fake = DEFAULT_FAKE
        self.logger = DEFAULT_LOGGER

    def _pg_bulk_copy(self, conn, df: pl.DataFrame, table_name: str):
        if df.is_empty():
            return
        buffer = BytesIO()
        df.write_csv(buffer, include_header=False, separator="|")
        buffer.seek(0)
        with conn.cursor() as cursor:
            cursor.copy_from(buffer, table_name, sep="|", columns=df.columns)
        conn.commit()

    def _create_schema(self, cur):
        self.logger.info("SCHEMA: Initializing Relational Tables with Audit Columns...")
        cur.execute(
            """
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
                quantity INTEGER,
                unit_price DECIMAL(12,2)
            );
            """
        )

    def _generate_customer_data(self, company_cuit, backdate_from="-1y"):
        fn, ln = self.fake.first_name(), self.fake.last_name()
        username = self.fake.unique.bothify(f"{fn[:1].lower()}_{ln.lower()}_#####")
        return {
            "company_cuit": company_cuit,
            "document_number": self.fake.unique.bothify("########"),
            "username": username,
            "first_name": fn,
            "last_name": ln,
            "email": f"{username}@{self.fake.free_email_domain()}",
            "phone_number": self.fake.phone_number(),
            "birth_date": self.fake.date_of_birth(minimum_age=18, maximum_age=80),
            "created_at": self.fake.date_time_between(start_date=backdate_from, end_date="now"),
        }

    def _sample_base_price(self) -> float:
        min_price, max_price = ordered_bounds(float(self.params.min_base_price), float(self.params.max_base_price))
        return round(random.uniform(min_price, max_price), 2)

    def _sample_items_per_order(self) -> int:
        min_items, max_items = ordered_bounds(
            int(self.params.min_items_per_order), int(self.params.max_items_per_order)
        )
        return random.randint(min_items, max_items)

    def _sample_item_quantity(self) -> int:
        min_qty, max_qty = ordered_bounds(int(self.params.min_item_quantity), int(self.params.max_item_quantity))
        return random.randint(min_qty, max_qty)

    def _build_calendar_month_probabilities(self) -> np.ndarray:
        return build_month_probability_vector(
            base_month_weights=self.params.base_month_weights,
            seasonality_amplitude=self.params.seasonality_amplitude,
            seasonality_peak_month=self.params.seasonality_peak_month,
            month_jitter_sigma=self.params.month_jitter_sigma,
        )

    def _sample_incremental_order_volume(self, now_ts: datetime) -> int:
        return sample_seasonal_volume(
            min_count=self.params.min_inc_orders,
            max_count=self.params.max_inc_orders,
            now_ts=now_ts,
            base_month_weights=self.params.base_month_weights,
            seasonality_amplitude=self.params.seasonality_amplitude,
            seasonality_peak_month=self.params.seasonality_peak_month,
            volume_jitter_sigma=self.params.incremental_volume_jitter_sigma,
            min_factor=self.params.min_incremental_volume_factor,
            max_factor=self.params.max_incremental_volume_factor,
        )

    def _get_distribution_window_start(self, now_ts: datetime, is_seed: bool) -> datetime:
        window_days = self.params.seed_distribution_days if is_seed else self.params.incremental_distribution_days
        return now_ts - timedelta(days=max(1, int(window_days)))

    def _sample_order_date_with_temporal_pattern(
        self, customer_created_at: datetime | None, now_ts: datetime, month_probs: np.ndarray, is_seed: bool
    ) -> datetime:
        window_start = self._get_distribution_window_start(now_ts, is_seed)
        if customer_created_at is None:
            customer_created_at = window_start
        effective_start = max(customer_created_at, window_start)
        if effective_start >= now_ts:
            return now_ts
        return sample_timestamp_within_window(
            window_start=effective_start,
            window_end=now_ts,
            month_probs=month_probs,
            intra_month_skew_alpha=self.params.intra_month_skew_alpha,
            intra_month_skew_beta=self.params.intra_month_skew_beta,
            day_jitter_std=self.params.day_jitter_std,
            hour_jitter_std=self.params.hour_jitter_std,
            # Preserve existing behavior where jitter can cross month boundaries.
            clamp_jitter_to_bucket=False,
        )

    def _generate_batch(self, conn, num_orders=1000, is_seed=False):
        stats = {"cust": 0, "prod": 0, "returns": 0, "new_co": 0, "price_upd": 0}

        with conn.cursor() as cur:
            if not is_seed:
                cur.execute("SELECT cuit, type FROM companies")
                all_cos = cur.fetchall()
                client_cuits = [c[0] for c in all_cos if c[1] == "Client"]
                sup_cuits = [c[0] for c in all_cos if c[1] == "Supplier"]

                num_slots = random.randint(self.params.min_event_slots, self.params.max_event_slots)
                for _ in range(num_slots):
                    if random.random() < self.params.prob_new_company:
                        new_cuit = self.fake.unique.bothify("##-########-#")
                        co_type = random.choices(
                            ["Supplier", "Client"],
                            weights=[self.params.prob_supplier, self.params.prob_client],
                        )[0]
                        now_ts = datetime.now()
                        cur.execute(
                            "INSERT INTO companies (cuit, name, type, country_code, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
                            (
                                new_cuit,
                                self.fake.company(),
                                co_type,
                                random.choice([c["code"] for c in get_iso_data()]),
                                now_ts,
                                now_ts,
                            ),
                        )

                        if co_type == "Client":
                            cur.execute(
                                "SELECT id, base_price FROM products ORDER BY RANDOM() LIMIT %s",
                                (self.params.catalog_size_seed,),
                            )
                            p_list = cur.fetchall()
                            self._pg_bulk_copy(
                                conn,
                                pl.DataFrame(
                                    [
                                        {
                                            "company_cuit": new_cuit,
                                            "product_id": p[0],
                                            "sale_price": round(float(p[1]) * 1.5, 2),
                                            "created_at": now_ts,
                                            "updated_at": now_ts,
                                        }
                                        for p in p_list
                                    ]
                                ),
                                "company_catalogs",
                            )
                            client_cuits.append(new_cuit)
                        else:
                            self._pg_bulk_copy(
                                conn,
                                pl.DataFrame(
                                    [
                                        {
                                            "name": f"SKU_{self.fake.word()}",
                                            "supplier_cuit": new_cuit,
                                            "base_price": self._sample_base_price(),
                                            "created_at": now_ts,
                                            "updated_at": now_ts,
                                        }
                                        for _ in range(max(1, int(self.params.new_supplier_products)))
                                    ]
                                ),
                                "products",
                            )
                            sup_cuits.append(new_cuit)
                        stats["new_co"] += 1

                    if random.random() < self.params.prob_price_update:
                        target_sup = random.choice(sup_cuits)
                        cur.execute(
                            "UPDATE products SET base_price = base_price * %s, updated_at = NOW() WHERE supplier_cuit = %s",
                            (random.uniform(0.95, 1.10), target_sup),
                        )
                        cur.execute(
                            """
                            UPDATE company_catalogs cc
                            SET sale_price = p.base_price * 1.4, updated_at = NOW()
                            FROM products p WHERE cc.product_id = p.id AND p.supplier_cuit = %s
                            """,
                            (target_sup,),
                        )
                        stats["price_upd"] += 1

                num_new_custs = max(
                    1,
                    int(
                        len(client_cuits)
                        * np.random.uniform(self.params.min_target_cust_growth, self.params.max_target_cust_growth)
                    ),
                )
                self._pg_bulk_copy(
                    conn,
                    pl.DataFrame(
                        [self._generate_customer_data(random.choice(client_cuits), "-1d") for _ in range(num_new_custs)]
                    ),
                    "customers",
                )

                num_new_prods = max(
                    1,
                    int(
                        self.params.seed_total_products
                        * np.random.uniform(self.params.min_target_prod_growth, self.params.max_target_prod_growth)
                    ),
                )
                now_ts = datetime.now()
                self._pg_bulk_copy(
                    conn,
                    pl.DataFrame(
                        [
                            {
                                "name": f"Prod_{self.fake.word()}",
                                "supplier_cuit": random.choice(sup_cuits),
                                "base_price": self._sample_base_price(),
                                "created_at": now_ts,
                                "updated_at": now_ts,
                            }
                            for _ in range(num_new_prods)
                        ]
                    ),
                    "products",
                )
                stats.update({"cust": num_new_custs, "prod": num_new_prods})

                window_start, window_end = datetime.now() - timedelta(days=7), datetime.now() - timedelta(days=3)
                cur.execute(
                    "SELECT id FROM orders WHERE status = 'COMPLETED' AND order_date BETWEEN %s AND %s",
                    (window_start, window_end),
                )
                eligible = [r[0] for r in cur.fetchall()]
                if eligible:
                    to_ret = random.sample(
                        eligible,
                        max(
                            1,
                            int(
                                len(eligible) * random.uniform(self.params.min_return_rate, self.params.max_return_rate)
                            ),
                        ),
                    )
                    cur.execute(
                        "UPDATE orders SET status = 'RETURNED', updated_at = NOW() WHERE id = ANY(%s)", (to_ret,)
                    )
                    stats["returns"] = len(to_ret)

            cur.execute("SELECT product_id, company_cuit, sale_price FROM company_catalogs")
            catalogs = {}
            for pid, cuit, price in cur.fetchall():
                catalogs.setdefault(cuit, []).append((pid, float(price)))
            cur.execute("SELECT id, company_cuit, created_at FROM customers")
            cust_info = {r[0]: {"cuit": r[1], "created_at": r[2]} for r in cur.fetchall()}

        order_list, items_by_order = [], []
        now_ts = datetime.now()
        calendar_month_probs = self._build_calendar_month_probabilities()
        valid_customers = []
        for cid, meta in cust_info.items():
            cat = catalogs.get(meta["cuit"], [])
            if not cat:
                continue
            created_at = meta["created_at"] if meta["created_at"] is not None else datetime.min
            valid_customers.append({"id": cid, "cuit": meta["cuit"], "created_at": created_at})

        if not valid_customers:
            self.logger.warning("BATCH: No eligible customers with catalogs found. Skipping order generation.")
            return

        valid_customers.sort(key=lambda x: x["created_at"])
        sampled_order_dates = [
            self._sample_order_date_with_temporal_pattern(
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
                weights=self.params.seed_status_weights if is_seed else [0.97, 0.03, 0],
            )[0]

            num_items = self._sample_items_per_order()
            selected = np.random.choice(len(cat), size=min(num_items, len(cat)), replace=False)
            total, current_items = 0, []
            for idx in selected:
                pid, price = cat[idx]
                qty = self._sample_item_quantity()
                total += qty * price
                current_items.append({"product_id": pid, "quantity": qty, "unit_price": price})

            order_list.append(
                {
                    "company_cuit": customer_cuit,
                    "customer_id": cid,
                    "status": status,
                    "order_date": o_date,
                    "total_amount": round(total, 2),
                    "created_at": o_date,
                    # Keep seed rows historical, but stamp incremental inserts as "now"
                    # so incremental_timestamp extraction does not miss backdated orders.
                    "updated_at": o_date if is_seed else now_ts,
                }
            )
            items_by_order.append(current_items)

        if len(order_list) < num_orders:
            self.logger.warning(
                f"BATCH: Generated {len(order_list)} of {num_orders} requested orders due to customer eligibility by date."
            )

        self._pg_bulk_copy(conn, pl.DataFrame(order_list), "orders")

        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM orders ORDER BY id DESC LIMIT {len(order_list)}")
            new_ids = [r[0] for r in cur.fetchall()][::-1]

        final_items = []
        for i, oid in enumerate(new_ids):
            for item in items_by_order[i]:
                item["order_id"] = oid
                final_items.append(item)
        self._pg_bulk_copy(conn, pl.DataFrame(final_items), "order_items")

        self.logger.info(
            f"BATCH SUMMARY: +{stats['new_co']} Co, +{stats['cust']} Cust, "
            f"+{stats['prod']} Prod, +{stats['price_upd']} PriceShift, "
            f"+{len(order_list)} Orders (+{len(final_items)} Items)"
        )

    @timed_run
    def generate(self):
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'orders')")
                initialized = cur.fetchone()[0]

            if not initialized:
                self.logger.info("SEED: Generating 12-Month Relational Baseline...")
                with conn.cursor() as cur:
                    self._create_schema(cur)
                iso = get_iso_data()
                self._pg_bulk_copy(conn, pl.DataFrame(iso), "ref_countries")

                num_sup = int(self.params.seed_total_companies * self.params.seed_supplier_ratio)
                cos = []
                for i in range(self.params.seed_total_companies):
                    ts = self.fake.date_time_between("-2y", "-1y")
                    cos.append(
                        {
                            "cuit": self.fake.unique.bothify("##-########-#"),
                            "name": self.fake.company(),
                            "type": "Supplier" if i < num_sup else "Client",
                            "country_code": random.choice([c["code"] for c in iso]),
                            "created_at": ts,
                            "updated_at": ts,
                        }
                    )
                self._pg_bulk_copy(conn, pl.DataFrame(cos), "companies")

                sup_cuits = [c["cuit"] for c in cos if c["type"] == "Supplier"]
                prods = []
                for i in range(self.params.seed_total_products):
                    ts = self.fake.date_time_between("-1y", "-10m")
                    prods.append(
                        {
                            "name": f"P_{i}",
                            "supplier_cuit": random.choice(sup_cuits),
                            "base_price": self._sample_base_price(),
                            "created_at": ts,
                            "updated_at": ts,
                        }
                    )
                self._pg_bulk_copy(conn, pl.DataFrame(prods), "products")

                with conn.cursor() as cur:
                    cur.execute("SELECT id, base_price, created_at FROM products")
                    all_p = cur.fetchall()
                    client_cuits = [c["cuit"] for c in cos if c["type"] == "Client"]
                    cats = []
                    for cuit in client_cuits:
                        selected = np.random.choice(len(all_p), self.params.catalog_size_seed, replace=False)
                        for idx in selected:
                            pid, bp, p_ts = all_p[idx]
                            cats.append(
                                {
                                    "company_cuit": cuit,
                                    "product_id": pid,
                                    "sale_price": round(
                                        float(bp)
                                        * random.uniform(
                                            self.params.min_markup_percent,
                                            self.params.max_markup_percent,
                                        ),
                                        2,
                                    ),
                                    "created_at": p_ts,
                                    "updated_at": p_ts,
                                }
                            )
                    self._pg_bulk_copy(conn, pl.DataFrame(cats), "company_catalogs")
                    self._pg_bulk_copy(
                        conn,
                        pl.DataFrame(
                            [
                                self._generate_customer_data(random.choice(client_cuits))
                                for _ in range(self.params.seed_total_customers)
                            ]
                        ),
                        "customers",
                    )

                self._generate_batch(conn, num_orders=self.params.seed_total_orders, is_seed=True)
            else:
                self.logger.info("EVOLUTION: Running Dynamic Market Cycle...")
                inc_orders = self._sample_incremental_order_volume(datetime.now())
                self.logger.info(f"EVOLUTION: Seasonal daily order target = {inc_orders}")
                self._generate_batch(conn, num_orders=inc_orders, is_seed=False)
        finally:
            conn.close()


if __name__ == "__main__":
    csdg = CommerceSourceDataGenerator()
    csdg.generate()
