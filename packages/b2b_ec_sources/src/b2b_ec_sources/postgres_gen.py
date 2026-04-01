"""Relational source generator for seed baseline and ongoing market evolution."""

import random
from datetime import datetime, timedelta
from io import BytesIO

import numpy as np
import polars as pl
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.timer import timed_run
from faker import Faker

from b2b_ec_sources import get_connection, get_iso_data
from b2b_ec_sources.geography import (
    build_country_distribution,
    classify_trade_lane,
    country_sampling_weights,
    localized_company_name,
    localized_first_last_name,
    localized_phone,
    sample_country_code,
    sample_trade_lane,
)
from b2b_ec_sources.temporal_sampling import (
    build_month_probability_vector,
    ordered_bounds,
    sample_seasonal_volume,
    sample_timestamp_within_window,
)
from b2b_ec_sources.source_db_gen import (
    CSDGParameters,
    CORE_SCHEMA_SQL,
    DEFAULT_CSDG_PARAMS,
    LEAD_CONVERSIONS_SCHEMA_SQL,
    apply_lead_conversions,
    build_order_payload,
    persist_orders_and_items,
)
DEFAULT_FAKE = Faker()
DEFAULT_LOGGER = get_logger("SourceDBGeneration")


class CommerceSourceDataGenerator:
    def __init__(self, params: CSDGParameters | None = None):
        """Initialize generator dependencies and runtime parameters."""
        self.params = params or DEFAULT_CSDG_PARAMS
        self.fake = DEFAULT_FAKE
        self.logger = DEFAULT_LOGGER

    def _pg_bulk_copy(self, conn, df: pl.DataFrame, table_name: str):
        """Copy a Polars dataframe into Postgres using COPY FROM for performance."""
        if df.is_empty():
            return
        buffer = BytesIO()
        df.write_csv(buffer, include_header=False, separator="|")
        buffer.seek(0)
        with conn.cursor() as cursor:
            cursor.copy_from(buffer, table_name, sep="|", columns=df.columns)
        conn.commit()

    def _pg_bulk_copy_rows(self, conn, rows: list[dict], table_name: str):
        """Convenience wrapper for bulk-copying list-of-dict payloads."""
        if not rows:
            return
        self._pg_bulk_copy(conn, pl.DataFrame(rows), table_name)

    def _create_schema(self, cur):
        """Create all relational source tables if they do not exist."""
        self.logger.info("SCHEMA: Initializing Relational Tables with Audit Columns...")
        cur.execute(CORE_SCHEMA_SQL)
        self._ensure_lead_conversion_schema(cur)

    def _generate_customer_data(self, company_cuit, company_country_code=None, backdate_from="-1y"):
        """Generate one customer row aligned to a company and country locale."""
        fn, ln = localized_first_last_name(company_country_code)
        username = self.fake.unique.bothify("u######_#####")
        return {
            "company_cuit": company_cuit,
            "document_number": self.fake.unique.bothify("########"),
            "username": username,
            "first_name": fn,
            "last_name": ln,
            "email": f"{username}@{self.fake.free_email_domain()}",
            "phone_number": localized_phone(company_country_code),
            "birth_date": self.fake.date_of_birth(minimum_age=18, maximum_age=80),
            "created_at": self.fake.date_time_between(start_date=backdate_from, end_date="now"),
        }

    def _sample_base_price(self) -> float:
        """Sample one product base price within configured bounds."""
        min_price, max_price = ordered_bounds(float(self.params.min_base_price), float(self.params.max_base_price))
        return round(random.uniform(min_price, max_price), 2)

    def _sample_items_per_order(self) -> int:
        """Sample item count for a single order."""
        min_items, max_items = ordered_bounds(
            int(self.params.min_items_per_order), int(self.params.max_items_per_order)
        )
        return random.randint(min_items, max_items)

    def _sample_item_quantity(self) -> int:
        """Sample quantity for one order line."""
        min_qty, max_qty = ordered_bounds(int(self.params.min_item_quantity), int(self.params.max_item_quantity))
        return random.randint(min_qty, max_qty)

    def _build_calendar_month_probabilities(self) -> np.ndarray:
        """Build month probability vector used by temporal samplers."""
        return build_month_probability_vector(
            base_month_weights=self.params.base_month_weights,
            seasonality_amplitude=self.params.seasonality_amplitude,
            seasonality_peak_month=self.params.seasonality_peak_month,
            month_jitter_sigma=self.params.month_jitter_sigma,
        )

    def _sample_incremental_order_volume(self, now_ts: datetime) -> int:
        """Sample daily incremental order count with seasonal weighting."""
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
        """Return lower time bound for seed/evolution order date sampling."""
        window_days = self.params.seed_distribution_days if is_seed else self.params.incremental_distribution_days
        return now_ts - timedelta(days=max(1, int(window_days)))

    def _sample_order_date_with_temporal_pattern(
        self, customer_created_at: datetime | None, now_ts: datetime, month_probs: np.ndarray, is_seed: bool
    ) -> datetime:
        """Sample one order timestamp constrained by customer lifecycle and temporal shape."""
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

    def _sample_supplier_cuit(self, supplier_cuits, company_country_by_cuit, country_distribution):
        """Sample supplier with country-aware weighting from the configured distribution."""
        if not supplier_cuits:
            raise ValueError("supplier_cuits cannot be empty")
        supplier_countries = [company_country_by_cuit.get(cuit, "") for cuit in supplier_cuits]
        weights = country_sampling_weights(supplier_countries, distribution=country_distribution)
        return random.choices(supplier_cuits, weights=weights, k=1)[0]

    def _fetch_products_with_supplier_country(self, cur, company_country_by_cuit):
        """Fetch products and enrich each with supplier country code."""
        cur.execute("SELECT id, base_price, created_at, supplier_cuit FROM products")
        return [
            {
                "id": row[0],
                "base_price": float(row[1]),
                "created_at": row[2],
                "supplier_cuit": row[3],
                "supplier_country_code": company_country_by_cuit.get(row[3]),
            }
            for row in cur.fetchall()
        ]

    def _fetch_catalogs(self, cur):
        """Fetch catalogs as company->[(product_id, sale_price)] mapping."""
        cur.execute("SELECT product_id, company_cuit, sale_price FROM company_catalogs")
        catalogs = {}
        for pid, cuit, price in cur.fetchall():
            catalogs.setdefault(cuit, []).append((pid, float(price)))
        return catalogs

    def _fetch_customer_info(self, cur):
        """Fetch customer info keyed by customer id for order synthesis."""
        cur.execute("SELECT id, company_cuit, created_at FROM customers")
        return {row[0]: {"cuit": row[1], "created_at": row[2]} for row in cur.fetchall()}

    def _index_client_cuits_by_name(self, all_companies: list[tuple]) -> dict[str, str | None]:
        """
        Build name->client_cuit map while marking duplicate names as ambiguous.
        Ambiguous names are stored as None to prevent accidental cross-company matching.
        """
        mapping: dict[str, str | None] = {}
        ambiguous: set[str] = set()
        for cuit, name, company_type, _ in all_companies:
            if company_type != "Client" or not name:
                continue
            key = str(name).strip().lower()
            if not key:
                continue
            if key in ambiguous:
                continue
            existing = mapping.get(key)
            if existing and existing != cuit:
                mapping[key] = None
                ambiguous.add(key)
                continue
            mapping.setdefault(key, cuit)
        return mapping

    def _build_catalog_entries_for_client(self, client_cuit, client_country_code, product_rows, now_ts):
        """Build one client's catalog using domestic/regional/global trade-lane sampling."""
        target_size = min(self.params.catalog_size_seed, len(product_rows))
        if target_size <= 0:
            return []

        lane_buckets = {"domestic": [], "regional": [], "global": []}
        for row in product_rows:
            lane = classify_trade_lane(client_country_code, row.get("supplier_country_code"))
            lane_buckets[lane].append(row)

        selected_rows = []
        used_product_ids = set()
        all_lanes = ("domestic", "regional", "global")

        for _ in range(target_size):
            preferred_lane = sample_trade_lane()
            lane_priority = [preferred_lane] + [lane for lane in all_lanes if lane != preferred_lane]
            picked_row = None

            for lane in lane_priority:
                candidates = [row for row in lane_buckets[lane] if row["id"] not in used_product_ids]
                if candidates:
                    picked_row = random.choice(candidates)
                    break

            if picked_row is None:
                remaining = [row for row in product_rows if row["id"] not in used_product_ids]
                if not remaining:
                    break
                picked_row = random.choice(remaining)

            used_product_ids.add(picked_row["id"])
            product_created_at = picked_row.get("created_at") or now_ts
            selected_rows.append(
                {
                    "company_cuit": client_cuit,
                    "product_id": picked_row["id"],
                    "sale_price": round(
                        float(picked_row["base_price"])
                        * random.uniform(self.params.min_markup_percent, self.params.max_markup_percent),
                        2,
                    ),
                    "created_at": product_created_at,
                    "updated_at": product_created_at,
                }
            )

        return selected_rows

    def _ensure_lead_conversion_schema(self, cur):
        """Ensure conversion audit schema exists (used for both seed creation and evolution backfill)."""
        cur.execute(LEAD_CONVERSIONS_SCHEMA_SQL)

    def _apply_lead_conversions(
        self,
        conn,
        cur,
        stats: dict,
        client_cuits: list[str],
        company_country_by_cuit: dict[str, str | None],
        client_cuit_by_name: dict[str, str | None],
        country_distribution: dict[str, object],
    ) -> None:
        """Delegate lead-conversion execution to the extracted conversion module."""
        apply_lead_conversions(
            conn=conn,
            cur=cur,
            params=self.params,
            stats=stats,
            logger=self.logger,
            fake=self.fake,
            client_cuits=client_cuits,
            company_country_by_cuit=company_country_by_cuit,
            client_cuit_by_name=client_cuit_by_name,
            country_distribution=country_distribution,
            fetch_products_with_supplier_country=self._fetch_products_with_supplier_country,
            build_catalog_entries_for_client=self._build_catalog_entries_for_client,
            pg_bulk_copy_rows=self._pg_bulk_copy_rows,
            generate_customer_data=self._generate_customer_data,
        )

    def _evolve_market_state(self, conn, cur, stats, country_distribution):
        """Mutate company/product/customer state for one evolution run."""
        # Step 1: capture current company state used by all evolution mutations.
        cur.execute("SELECT cuit, name, type, country_code FROM companies")
        all_cos = cur.fetchall()
        client_cuits = [c[0] for c in all_cos if c[2] == "Client"]
        sup_cuits = [c[0] for c in all_cos if c[2] == "Supplier"]
        company_country_by_cuit = {c[0]: c[3] for c in all_cos}
        client_cuit_by_name = self._index_client_cuits_by_name(all_cos)

        # Step 2: run a small number of random event slots (new company / price shift).
        num_slots = random.randint(self.params.min_event_slots, self.params.max_event_slots)
        for _ in range(num_slots):
            if random.random() < self.params.prob_new_company:
                new_cuit = self.fake.unique.bothify("##-########-#")
                co_type = random.choices(
                    ["Supplier", "Client"],
                    weights=[self.params.prob_supplier, self.params.prob_client],
                )[0]
                new_country_code = sample_country_code(country_distribution)
                new_company_name = localized_company_name(new_country_code)
                now_ts = datetime.now()
                cur.execute(
                    "INSERT INTO companies (cuit, name, type, country_code, created_at, updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
                    (
                        new_cuit,
                        new_company_name,
                        co_type,
                        new_country_code,
                        now_ts,
                        now_ts,
                    ),
                )
                company_country_by_cuit[new_cuit] = new_country_code

                if co_type == "Client":
                    p_list = self._fetch_products_with_supplier_country(cur, company_country_by_cuit)
                    catalog_rows = self._build_catalog_entries_for_client(
                        client_cuit=new_cuit,
                        client_country_code=new_country_code,
                        product_rows=p_list,
                        now_ts=now_ts,
                    )
                    self._pg_bulk_copy_rows(conn, catalog_rows, "company_catalogs")
                    client_cuits.append(new_cuit)
                    name_key = new_company_name.strip().lower()
                    existing_cuit = client_cuit_by_name.get(name_key)
                    if existing_cuit is None and name_key in client_cuit_by_name:
                        pass
                    elif existing_cuit and existing_cuit != new_cuit:
                        client_cuit_by_name[name_key] = None
                    else:
                        client_cuit_by_name[name_key] = new_cuit
                else:
                    self._pg_bulk_copy_rows(
                        conn,
                        [
                            {
                                "name": f"SKU_{self.fake.word()}",
                                "supplier_cuit": new_cuit,
                                "base_price": self._sample_base_price(),
                                "created_at": now_ts,
                                "updated_at": now_ts,
                            }
                            for _ in range(max(1, int(self.params.new_supplier_products)))
                        ],
                        "products",
                    )
                    sup_cuits.append(new_cuit)
                stats["new_co"] += 1

            if sup_cuits and random.random() < self.params.prob_price_update:
                target_sup = self._sample_supplier_cuit(sup_cuits, company_country_by_cuit, country_distribution)
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

        # Step 3: convert eligible marketing prospects into real client entities.
        self._apply_lead_conversions(
            conn=conn,
            cur=cur,
            stats=stats,
            client_cuits=client_cuits,
            company_country_by_cuit=company_country_by_cuit,
            client_cuit_by_name=client_cuit_by_name,
            country_distribution=country_distribution,
        )

        # Step 4: apply steady-state customer growth against current client base.
        if client_cuits:
            num_new_custs = max(
                1,
                int(
                    len(client_cuits)
                    * np.random.uniform(self.params.min_target_cust_growth, self.params.max_target_cust_growth)
                ),
            )
            client_weights = country_sampling_weights(
                [company_country_by_cuit.get(cuit, "") for cuit in client_cuits],
                distribution=country_distribution,
            )
            customer_company_cuits = random.choices(client_cuits, weights=client_weights, k=num_new_custs)
            self._pg_bulk_copy_rows(
                conn,
                [
                    self._generate_customer_data(
                        company_cuit=cuit,
                        company_country_code=company_country_by_cuit.get(cuit),
                        backdate_from="-1d",
                    )
                    for cuit in customer_company_cuits
                ],
                "customers",
            )
            stats["cust"] = num_new_custs
        else:
            self.logger.warning("EVOLUTION: No client companies available for customer growth.")

        # Step 5: apply steady-state product growth against current supplier base.
        if sup_cuits:
            num_new_prods = max(
                1,
                int(
                    self.params.seed_total_products
                    * np.random.uniform(self.params.min_target_prod_growth, self.params.max_target_prod_growth)
                ),
            )
            now_ts = datetime.now()
            self._pg_bulk_copy_rows(
                conn,
                [
                    {
                        "name": f"Prod_{self.fake.word()}",
                        "supplier_cuit": self._sample_supplier_cuit(
                            sup_cuits, company_country_by_cuit, country_distribution
                        ),
                        "base_price": self._sample_base_price(),
                        "created_at": now_ts,
                        "updated_at": now_ts,
                    }
                    for _ in range(num_new_prods)
                ],
                "products",
            )
            stats["prod"] = num_new_prods
        else:
            self.logger.warning("EVOLUTION: No supplier companies available for product growth.")

        # Step 6: mark a portion of older completed orders as returned.
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
                    int(len(eligible) * random.uniform(self.params.min_return_rate, self.params.max_return_rate)),
                ),
            )
            cur.execute("UPDATE orders SET status = 'RETURNED', updated_at = NOW() WHERE id = ANY(%s)", (to_ret,))
            stats["returns"] = len(to_ret)

    def _build_order_payload(self, catalogs, cust_info, num_orders, is_seed):
        """Delegate order-payload synthesis to the extracted orders module."""
        return build_order_payload(
            catalogs=catalogs,
            cust_info=cust_info,
            num_orders=num_orders,
            is_seed=is_seed,
            params=self.params,
            logger=self.logger,
            build_calendar_month_probabilities=self._build_calendar_month_probabilities,
            sample_order_date_with_temporal_pattern=self._sample_order_date_with_temporal_pattern,
            sample_items_per_order=self._sample_items_per_order,
            sample_item_quantity=self._sample_item_quantity,
        )

    def _persist_orders_and_items(self, conn, order_list, items_by_order):
        """Delegate order/item persistence to the extracted orders module."""
        return persist_orders_and_items(
            conn=conn,
            order_list=order_list,
            items_by_order=items_by_order,
            pg_bulk_copy_rows=self._pg_bulk_copy_rows,
        )

    def _generate_batch(self, conn, num_orders=1000, is_seed=False, base_stats=None):
        """Run one order-generation batch, optionally including evolution-side mutations."""
        stats = {
            "cust": 0,
            "prod": 0,
            "returns": 0,
            "new_co": 0,
            "price_upd": 0,
            "lead_conv": 0,
            "lead_conv_new_clients": 0,
            "lead_conv_new_customers": 0,
        }
        if base_stats:
            stats.update(base_stats)
        country_distribution = build_country_distribution()

        # Step 1: refresh state snapshots; evolution mode mutates entity state before order generation.
        with conn.cursor() as cur:
            if not is_seed:
                self._evolve_market_state(conn, cur, stats, country_distribution)

            catalogs = self._fetch_catalogs(cur)
            cust_info = self._fetch_customer_info(cur)

        # Step 2: build and persist transactional payloads.
        order_list, items_by_order = self._build_order_payload(catalogs, cust_info, num_orders, is_seed)
        order_count, item_count = self._persist_orders_and_items(conn, order_list, items_by_order)

        # Step 3: emit concise run telemetry for monitoring.
        mode_label = "SEED" if is_seed else "EVOLUTION"
        self.logger.info(
            f"BATCH SUMMARY [{mode_label}]: +{stats['new_co']} Co, +{stats['cust']} Cust, "
            f"+{stats['prod']} Prod, +{stats['price_upd']} PriceShift, +{stats['returns']} Returns, "
            f"+{stats['lead_conv']} LeadConv (+{stats['lead_conv_new_clients']} NewClients, "
            f"+{stats['lead_conv_new_customers']} ConvCustomers), "
            f"+{order_count} Orders (+{item_count} Items)"
        )

    @timed_run
    def generate(self):
        """Run seed initialization once, then evolution-mode updates on later runs."""
        conn = get_connection()
        try:
            # Step 1: detect whether source tables already exist.
            with conn.cursor() as cur:
                cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'orders')")
                initialized = cur.fetchone()[0]

            if not initialized:
                # Step 2 (seed): bootstrap schema + static reference data.
                self.logger.info("SEED: Generating 12-Month Relational Baseline...")
                with conn.cursor() as cur:
                    self._create_schema(cur)
                iso = get_iso_data()
                self._pg_bulk_copy(conn, pl.DataFrame(iso), "ref_countries")
                country_distribution = build_country_distribution()

                # Step 3 (seed): create core master entities (companies/products).
                num_sup = int(self.params.seed_total_companies * self.params.seed_supplier_ratio)
                cos = []
                for i in range(self.params.seed_total_companies):
                    ts = self.fake.date_time_between("-2y", "-1y")
                    country_code = sample_country_code(country_distribution)
                    cos.append(
                        {
                            "cuit": self.fake.unique.bothify("##-########-#"),
                            "name": localized_company_name(country_code),
                            "type": "Supplier" if i < num_sup else "Client",
                            "country_code": country_code,
                            "created_at": ts,
                            "updated_at": ts,
                        }
                    )
                self._pg_bulk_copy(conn, pl.DataFrame(cos), "companies")
                company_country_by_cuit = {c["cuit"]: c["country_code"] for c in cos}

                sup_cuits = [c["cuit"] for c in cos if c["type"] == "Supplier"]
                prods = []
                for i in range(self.params.seed_total_products):
                    ts = self.fake.date_time_between("-1y", "-10m")
                    supplier_cuit = self._sample_supplier_cuit(sup_cuits, company_country_by_cuit, country_distribution)
                    prods.append(
                        {
                            "name": f"P_{i}",
                            "supplier_cuit": supplier_cuit,
                            "base_price": self._sample_base_price(),
                            "created_at": ts,
                            "updated_at": ts,
                        }
                    )
                self._pg_bulk_copy(conn, pl.DataFrame(prods), "products")

                # Step 4 (seed): materialize dependent entities (catalogs/customers).
                with conn.cursor() as cur:
                    all_p = self._fetch_products_with_supplier_country(cur, company_country_by_cuit)
                    client_cuits = [c["cuit"] for c in cos if c["type"] == "Client"]
                    cats = []
                    for cuit in client_cuits:
                        cats.extend(
                            self._build_catalog_entries_for_client(
                                client_cuit=cuit,
                                client_country_code=company_country_by_cuit.get(cuit),
                                product_rows=all_p,
                                now_ts=datetime.now(),
                            )
                        )
                    self._pg_bulk_copy_rows(conn, cats, "company_catalogs")
                    client_weights = country_sampling_weights(
                        [company_country_by_cuit.get(cuit, "") for cuit in client_cuits],
                        distribution=country_distribution,
                    )
                    customer_company_cuits = random.choices(
                        client_cuits, weights=client_weights, k=self.params.seed_total_customers
                    )
                    seed_customer_count = len(customer_company_cuits)
                    self._pg_bulk_copy_rows(
                        conn,
                        [
                            self._generate_customer_data(
                                company_cuit=cuit,
                                company_country_code=company_country_by_cuit.get(cuit),
                            )
                            for cuit in customer_company_cuits
                        ],
                        "customers",
                    )

                self.logger.info(
                    f"SEED BASELINE: +{len(cos)} Co, +{seed_customer_count} Cust, "
                    f"+{len(prods)} Prod, +{len(cats)} CatalogRows"
                )
                # Step 5 (seed): generate initial transactional baseline.
                self._generate_batch(
                    conn,
                    num_orders=self.params.seed_total_orders,
                    is_seed=True,
                    base_stats={
                        "new_co": len(cos),
                        "cust": seed_customer_count,
                        "prod": len(prods),
                        "price_upd": 0,
                        "returns": 0,
                    },
                )
            else:
                # Step 2 (evolution): run one incremental market cycle.
                with conn.cursor() as cur:
                    self._ensure_lead_conversion_schema(cur)
                self.logger.info("EVOLUTION: Running Dynamic Market Cycle...")
                inc_orders = self._sample_incremental_order_volume(datetime.now())
                self.logger.info(f"EVOLUTION: Seasonal daily order target = {inc_orders}")
                self._generate_batch(conn, num_orders=inc_orders, is_seed=False)
        finally:
            conn.close()


if __name__ == "__main__":
    csdg = CommerceSourceDataGenerator()
    csdg.generate()
