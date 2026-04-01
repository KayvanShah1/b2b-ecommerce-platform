"""Relational source generator for seed baseline and ongoing market evolution."""

import random
from datetime import datetime, timedelta
from io import BytesIO

import numpy as np
import polars as pl
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.storage import storage
from b2b_ec_utils.timer import timed_run
from faker import Faker
from pydantic import BaseModel

from b2b_ec_sources import coerce_bool, get_connection, get_iso_data
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

    # Lead-to-client conversion (applied during evolution from latest lead snapshot file)
    enable_lead_conversion: bool = True
    lead_conversion_window_days: int = 7
    lead_conversion_max_candidates: int = 2500
    lead_conversion_base_new: float = 0.002
    lead_conversion_base_contacted: float = 0.008
    lead_conversion_base_nurturing: float = 0.010
    lead_conversion_base_qualified: float = 0.030
    lead_conversion_noise_sigma: float = 0.15
    lead_conversion_min_prob: float = 0.001
    lead_conversion_max_prob: float = 0.200
    lead_conversion_bootstrap_customer_ratio: float = 0.85

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
LEAD_CONVERSIONS_SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS lead_conversions (
        lead_id TEXT PRIMARY KEY,
        company_cuit TEXT NOT NULL REFERENCES companies(cuit),
        company_name TEXT NOT NULL,
        country_code TEXT REFERENCES ref_countries(code),
        lead_status TEXT,
        lead_source TEXT,
        conversion_probability DOUBLE PRECISION,
        converted_at TIMESTAMP NOT NULL,
        source_file TEXT,
        conversion_window_days INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT (now() at time zone 'utc')
    );
    CREATE INDEX IF NOT EXISTS idx_lead_conversions_company_cuit
        ON lead_conversions(company_cuit);
    CREATE INDEX IF NOT EXISTS idx_lead_conversions_converted_at
        ON lead_conversions(converted_at);
"""


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

    def _latest_marketing_leads_path(self) -> str | None:
        pattern = storage.get_marketing_leads_path("b2b_leads_*.csv")
        files = sorted(storage.glob(pattern))
        return files[-1] if files else None

    def _load_latest_marketing_leads(self) -> tuple[pl.DataFrame | None, str | None]:
        latest_path = self._latest_marketing_leads_path()
        if not latest_path:
            return None, None

        try:
            with storage.open(latest_path, mode="rb") as file_handle:
                return pl.read_csv(file_handle), latest_path
        except Exception as exc:
            self.logger.warning(f"LEAD CONVERSION SKIP: could not read {latest_path}: {exc}")
            return None, latest_path

    @staticmethod
    def _parse_datetime(value) -> datetime | None:
        if isinstance(value, datetime):
            return value
        if value is None:
            return None

        text = str(value).strip()
        if not text:
            return None

        normalized = text.replace("T", " ")
        if normalized.endswith("Z"):
            normalized = normalized[:-1]

        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(normalized, fmt)
            except ValueError:
                continue

        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
        except ValueError:
            return None

    def _lead_conversion_probability(self, row: dict, now_ts: datetime) -> float:
        status = str(row.get("status") or "").strip().lower()
        base_by_status = {
            "new": self.params.lead_conversion_base_new,
            "contacted": self.params.lead_conversion_base_contacted,
            "nurturing": self.params.lead_conversion_base_nurturing,
            "qualified": self.params.lead_conversion_base_qualified,
        }
        base_prob = base_by_status.get(status, 0.0)
        if base_prob <= 0.0:
            return 0.0

        source = str(row.get("lead_source") or "").strip().lower()
        source_multiplier = {
            "referral": 1.35,
            "webinar": 1.20,
            "trade show 2026": 1.15,
            "linkedin ads": 1.00,
            "google search": 1.00,
            "cold call": 0.80,
        }.get(source, 1.00)

        revenue_raw = row.get("estimated_annual_revenue")
        try:
            revenue = max(0.0, float(revenue_raw or 0.0))
        except (TypeError, ValueError):
            revenue = 0.0
        revenue_multiplier = 0.8 + min(1.0, revenue / 1_000_000.0) * 0.6

        anchor_ts = self._parse_datetime(row.get("status_updated_at")) or self._parse_datetime(row.get("created_at"))
        if anchor_ts is None:
            age_multiplier = 1.0
        else:
            age_days = max(0, (now_ts - anchor_ts).days)
            if age_days <= 3:
                age_multiplier = 1.15
            elif age_days <= 7:
                age_multiplier = 1.30
            elif age_days <= 14:
                age_multiplier = 0.85
            else:
                age_multiplier = 0.55

        noise = 1.0 + random.gauss(0.0, self.params.lead_conversion_noise_sigma)
        noise = min(1.5, max(0.5, noise))

        probability = base_prob * source_multiplier * revenue_multiplier * age_multiplier * noise
        return min(self.params.lead_conversion_max_prob, max(self.params.lead_conversion_min_prob, probability))

    def _customer_from_lead(self, lead_row: dict, company_cuit: str, country_code: str | None) -> dict:
        customer = self._generate_customer_data(
            company_cuit=company_cuit,
            company_country_code=country_code,
            backdate_from="-30d",
        )

        contact_name = str(lead_row.get("contact_name") or "").strip()
        if contact_name:
            name_parts = [part for part in contact_name.replace(",", " ").split() if part]
            if len(name_parts) == 1:
                customer["first_name"] = name_parts[0]
                customer["last_name"] = self.fake.last_name()
            elif len(name_parts) >= 2:
                customer["first_name"] = name_parts[0]
                customer["last_name"] = " ".join(name_parts[1:])

        contact_phone = str(lead_row.get("contact_phone") or "").strip()
        if contact_phone:
            customer["phone_number"] = contact_phone

        return customer

    def _apply_lead_conversions(
        self,
        conn,
        cur,
        stats: dict,
        client_cuits: list[str],
        company_country_by_cuit: dict[str, str | None],
        client_cuit_by_name: dict[str, str],
        country_distribution: dict[str, object],
    ) -> None:
        """Convert a sampled subset of prospect leads into client companies/customers."""
        if not self.params.enable_lead_conversion:
            return

        leads_df, source_file = self._load_latest_marketing_leads()
        if leads_df is None or leads_df.is_empty():
            self.logger.info("LEAD CONVERSION SKIP: no marketing leads snapshot found.")
            return

        if "lead_id" not in leads_df.columns:
            self.logger.warning("LEAD CONVERSION SKIP: lead_id column missing in latest leads snapshot.")
            return

        now_ts = datetime.now()
        cutoff_ts = now_ts - timedelta(days=max(1, int(self.params.lead_conversion_window_days)))

        candidate_rows = []
        for row in leads_df.to_dicts():
            lead_id = str(row.get("lead_id") or "").strip()
            company_name = str(row.get("company_name") or "").strip()
            status = str(row.get("status") or "").strip()
            if not lead_id or not company_name:
                continue
            if status.lower() == "lost":
                continue
            if not coerce_bool(row.get("is_prospect")):
                continue

            activity_ts = self._parse_datetime(row.get("status_updated_at")) or self._parse_datetime(row.get("created_at"))
            if activity_ts and activity_ts < cutoff_ts:
                continue

            candidate_rows.append(row)

        if self.params.lead_conversion_max_candidates > 0 and len(candidate_rows) > self.params.lead_conversion_max_candidates:
            candidate_rows = random.sample(candidate_rows, self.params.lead_conversion_max_candidates)

        if not candidate_rows:
            self.logger.info("LEAD CONVERSION SKIP: no eligible prospect leads in conversion window.")
            return

        cur.execute("SELECT lead_id FROM lead_conversions")
        converted_ids = {row[0] for row in cur.fetchall()}

        cur.execute("SELECT lower(email) FROM customers")
        existing_emails = {row[0] for row in cur.fetchall() if row[0]}

        cur.execute("SELECT code FROM ref_countries")
        valid_country_codes = {row[0] for row in cur.fetchall() if row[0]}

        product_rows = self._fetch_products_with_supplier_country(cur, company_country_by_cuit)

        conversion_rows = []
        new_customer_rows = []
        converted_count = 0
        new_client_count = 0
        new_customer_count = 0

        for lead in candidate_rows:
            lead_id = str(lead.get("lead_id") or "").strip()
            if lead_id in converted_ids:
                continue

            conversion_probability = self._lead_conversion_probability(lead, now_ts)
            if conversion_probability <= 0 or random.random() > conversion_probability:
                continue

            company_name = str(lead.get("company_name") or "").strip()
            company_key = company_name.lower()
            country_code = str(lead.get("country_code") or "").strip().upper() or sample_country_code(country_distribution)
            if country_code not in valid_country_codes:
                country_code = sample_country_code(country_distribution)

            company_cuit = client_cuit_by_name.get(company_key)
            if company_cuit:
                country_code = company_country_by_cuit.get(company_cuit) or country_code
            if not company_cuit:
                company_cuit = self.fake.unique.bothify("##-########-#")
                cur.execute(
                    """
                    INSERT INTO companies (cuit, name, type, country_code, created_at, updated_at)
                    VALUES (%s, %s, 'Client', %s, %s, %s)
                    """,
                    (company_cuit, company_name, country_code, now_ts, now_ts),
                )
                company_country_by_cuit[company_cuit] = country_code
                client_cuit_by_name[company_key] = company_cuit
                client_cuits.append(company_cuit)
                new_client_count += 1

                if product_rows:
                    catalog_rows = self._build_catalog_entries_for_client(
                        client_cuit=company_cuit,
                        client_country_code=country_code,
                        product_rows=product_rows,
                        now_ts=now_ts,
                    )
                    self._pg_bulk_copy_rows(conn, catalog_rows, "company_catalogs")

            if random.random() <= self.params.lead_conversion_bootstrap_customer_ratio:
                customer_row = self._customer_from_lead(
                    lead_row=lead,
                    company_cuit=company_cuit,
                    country_code=country_code,
                )
                lead_email = str(lead.get("contact_email") or "").strip().lower()
                if lead_email and lead_email not in existing_emails:
                    customer_row["email"] = lead_email
                    existing_emails.add(lead_email)
                new_customer_rows.append(customer_row)
                new_customer_count += 1

            conversion_rows.append(
                {
                    "lead_id": lead_id,
                    "company_cuit": company_cuit,
                    "company_name": company_name,
                    "country_code": country_code,
                    "lead_status": str(lead.get("status") or "").strip() or None,
                    "lead_source": str(lead.get("lead_source") or "").strip() or None,
                    "conversion_probability": round(float(conversion_probability), 6),
                    "converted_at": now_ts,
                    "source_file": source_file or None,
                    "conversion_window_days": int(self.params.lead_conversion_window_days),
                    "created_at": now_ts,
                }
            )
            converted_ids.add(lead_id)
            converted_count += 1

        if new_customer_rows:
            self._pg_bulk_copy_rows(conn, new_customer_rows, "customers")
        if conversion_rows:
            self._pg_bulk_copy_rows(conn, conversion_rows, "lead_conversions")

        stats["lead_conv"] = stats.get("lead_conv", 0) + converted_count
        stats["lead_conv_new_clients"] = stats.get("lead_conv_new_clients", 0) + new_client_count
        stats["lead_conv_new_customers"] = stats.get("lead_conv_new_customers", 0) + new_customer_count

        self.logger.info(
            f"LEAD CONVERSION: candidates={len(candidate_rows)} converted={converted_count} "
            f"new_clients={new_client_count} bootstrap_customers={new_customer_count}"
        )

    def _evolve_market_state(self, conn, cur, stats, country_distribution):
        """Mutate company/product/customer state for one evolution run."""
        # Step 1: capture current company state used by all evolution mutations.
        cur.execute("SELECT cuit, name, type, country_code FROM companies")
        all_cos = cur.fetchall()
        client_cuits = [c[0] for c in all_cos if c[2] == "Client"]
        sup_cuits = [c[0] for c in all_cos if c[2] == "Supplier"]
        company_country_by_cuit = {c[0]: c[3] for c in all_cos}
        client_cuit_by_name = {str(c[1]).strip().lower(): c[0] for c in all_cos if c[2] == "Client" and c[1]}

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
                    client_cuit_by_name[new_company_name.strip().lower()] = new_cuit
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
        """Build order and order-item payloads from current catalog/customer state."""
        order_list, items_by_order = [], []
        now_ts = datetime.now()
        calendar_month_probs = self._build_calendar_month_probabilities()
        valid_customers = []
        # Step 1: keep only customers whose company currently has a purchasable catalog.
        for cid, meta in cust_info.items():
            cat = catalogs.get(meta["cuit"], [])
            if not cat:
                continue
            created_at = meta["created_at"] if meta["created_at"] is not None else datetime.min
            valid_customers.append({"id": cid, "cuit": meta["cuit"], "created_at": created_at})

        if not valid_customers:
            self.logger.warning("BATCH: No eligible customers with catalogs found. Skipping order generation.")
            return order_list, items_by_order

        # Step 2: pre-sample and sort order dates so customer eligibility respects lifecycle timing.
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
        # Step 3: for each sampled date, pick an eligible customer and synthesize line items.
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
        return order_list, items_by_order

    def _persist_orders_and_items(self, conn, order_list, items_by_order):
        """Persist generated orders and items and return inserted counts."""
        if not order_list:
            return 0, 0

        self._pg_bulk_copy_rows(conn, order_list, "orders")

        with conn.cursor() as cur:
            cur.execute("SELECT id FROM orders ORDER BY id DESC LIMIT %s", (len(order_list),))
            new_ids = [r[0] for r in cur.fetchall()][::-1]

        final_items = []
        for i, oid in enumerate(new_ids):
            for item in items_by_order[i]:
                item["order_id"] = oid
                final_items.append(item)
        self._pg_bulk_copy_rows(conn, final_items, "order_items")
        return len(order_list), len(final_items)

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
