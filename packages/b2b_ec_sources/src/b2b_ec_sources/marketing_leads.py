import random
from datetime import datetime

import polars as pl
from b2b_ec_utils import get_logger, settings, timed_run
from b2b_ec_utils.storage import storage
from faker import Faker
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from b2b_ec_sources import get_connection, get_iso_data

fake = Faker()
logger = get_logger("MarketingLeadsGen")


class MarketingLeadsParameters(BaseSettings):
    sources: list[str] = [
        "LinkedIn Ads",
        "Google Search",
        "Trade Show 2026",
        "Webinar",
        "Cold Call",
        "Referral",
    ]
    statuses: list[str] = ["New", "Contacted", "Qualified", "Lost", "Nurturing"]

    existing_company_ratio: float = Field(default=0.70, ge=0.0, le=1.0)
    seed_leads_per_company: int = Field(default=4, ge=1)
    min_seed_leads: int = Field(default=200, ge=1)

    daily_leads_per_company_min: float = Field(default=0.5, ge=0.0)
    daily_leads_per_company_max: float = Field(default=1.5, ge=0.0)
    min_daily_leads: int = Field(default=50, ge=1)
    max_daily_leads: int = Field(default=1500, ge=1)

    carryover_ratio: float = Field(default=0.35, ge=0.0, le=1.0)
    fallback_iso_codes: list[str] = ["US", "IN", "QA", "AE", "GB", "DE"]

    model_config = SettingsConfigDict(env_prefix="MARKETING_LEADS_", extra="ignore")

    def model_post_init(self, __context):
        if self.daily_leads_per_company_min > self.daily_leads_per_company_max:
            raise ValueError("daily_leads_per_company_min must be <= daily_leads_per_company_max")
        if self.min_daily_leads > self.max_daily_leads:
            raise ValueError("min_daily_leads must be <= max_daily_leads")


MLG = MarketingLeadsParameters()


class MarketingLeadsGenerator:
    def __init__(self, params: MarketingLeadsParameters = MLG):
        self.params = params
        self.sources = params.sources
        self.statuses = params.statuses

        # Pull real ISO codes to ensure CSV joins perfectly with Postgres ref_countries
        try:
            self.iso_codes = [c["code"] for c in get_iso_data()]
        except Exception:
            # Fallback if DB is not initialized
            self.iso_codes = params.fallback_iso_codes

    def get_existing_companies(self):
        """Fetch existing company names to simulate 'Existing Client' leads."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM companies WHERE type = 'Client' LIMIT 500")
                return [r[0] for r in cur.fetchall()]
        except Exception as e:
            logger.warning(f"Could not fetch companies from DB: {e}. Using random names.")
            return []
        finally:
            conn.close()

    def _list_previous_leads_files(self, bucket: str):
        pattern = storage.get_path(bucket, "marketing", "b2b_leads_*.csv")
        files = storage.glob(pattern)
        return sorted(files)

    def _load_previous_leads(self, bucket: str):
        files = self._list_previous_leads_files(bucket)
        if not files:
            return None
        latest = files[-1]
        try:
            with storage.open(latest, mode="rb") as f:
                return pl.read_csv(f)
        except Exception as e:
            logger.warning(f"Could not read previous leads file {latest}: {e}")
            return None

    def _advance_status(self, current_status: str) -> str:
        if current_status == "Lost":
            return "Lost"
        roll = random.random()
        if roll < 0.15:
            return "Lost"
        if roll < 0.45:
            return "Contacted" if current_status == "New" else "Qualified"
        if roll < 0.65:
            return "Nurturing"
        return current_status

    def _suggest_count(self, client_count: int, is_seed: bool, prev_count: int) -> int:
        if is_seed:
            base = max(self.params.min_seed_leads, int(client_count * self.params.seed_leads_per_company))
            return int(base * random.uniform(0.8, 1.2))

        per_company = int(
            client_count
            * random.uniform(self.params.daily_leads_per_company_min, self.params.daily_leads_per_company_max)
        )
        from_previous = int(prev_count * random.uniform(0.05, 0.15)) if prev_count else 0
        base = max(self.params.min_daily_leads, per_company, from_previous)
        return min(self.params.max_daily_leads, base)

    def _new_lead_status(self, is_existing_company: bool) -> str:
        if is_existing_company:
            return random.choices(["Nurturing", "Qualified", "Contacted"], weights=[0.5, 0.3, 0.2])[0]
        return random.choices(["New", "Contacted", "Qualified"], weights=[0.6, 0.3, 0.1])[0]

    def _random_created_at(self, is_seed: bool) -> datetime:
        return fake.date_time_between(start_date="-1y" if is_seed else "-30d", end_date="now")

    @timed_run
    def generate(self, count: int | None = None):
        """Generates a B2B marketing leads CSV with Company-level data."""
        bucket = settings.storage.marketing_leads_bucket
        prev_df = self._load_previous_leads(bucket)
        is_seed = prev_df is None or prev_df.is_empty()

        existing_companies = self.get_existing_companies()
        prev_count = 0 if prev_df is None else prev_df.height
        target_count = count or self._suggest_count(len(existing_companies), is_seed, prev_count)

        logger.info(f"Generating {target_count} B2B marketing leads ({'SEED' if is_seed else 'DAILY'})")

        leads: list[dict] = []

        # 1) Carry over a subset of previous leads to keep timeline continuity
        carryover_count = 0
        if not is_seed and prev_df is not None and not prev_df.is_empty():
            carryover_count = min(int(target_count * self.params.carryover_ratio), prev_df.height)
            sample_df = prev_df.sample(n=carryover_count, with_replacement=False)
            for row in sample_df.to_dicts():
                current_status = row.get("status", "New")
                new_status = self._advance_status(current_status)
                status_changed = new_status != current_status

                created_at = row.get("created_at") or self._random_created_at(is_seed=True).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                status_updated_at = (
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S") if status_changed else row.get("status_updated_at")
                )
                last_activity_at = status_updated_at or row.get("last_activity_at") or created_at

                leads.append(
                    {
                        "lead_id": row.get("lead_id", fake.uuid4()),
                        "created_at": created_at,
                        "company_name": row.get("company_name", fake.company()),
                        "is_prospect": row.get("is_prospect", False),
                        "industry": row.get("industry", fake.bs().capitalize()),
                        "contact_name": row.get("contact_name", fake.name()),
                        "contact_email": row.get("contact_email", fake.unique.company_email()),
                        "contact_phone": row.get("contact_phone", fake.phone_number()),
                        "lead_source": row.get("lead_source", random.choice(self.sources)),
                        "estimated_annual_revenue": float(
                            row.get("estimated_annual_revenue", random.randint(50000, 1000000))
                        ),
                        "country_code": row.get("country_code", random.choice(self.iso_codes)),
                        "status": new_status,
                        "status_updated_at": status_updated_at,
                        "last_activity_at": last_activity_at,
                    }
                )

        # 2) Generate new leads for this batch
        new_count = target_count - carryover_count
        for _ in range(new_count):
            use_existing_company = bool(existing_companies) and random.random() < self.params.existing_company_ratio
            if use_existing_company:
                company_name = random.choice(existing_companies)
                is_prospect = False
            else:
                # New B2B prospect
                company_name = fake.company()
                is_prospect = True

            created_at_dt = self._random_created_at(is_seed=is_seed)
            created_at = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")
            status = self._new_lead_status(use_existing_company)
            status_updated_at = created_at
            last_activity_at = created_at

            leads.append(
                {
                    "lead_id": fake.uuid4(),
                    "created_at": created_at,
                    "company_name": company_name,
                    "is_prospect": is_prospect,
                    "industry": fake.bs().capitalize(),  # Generates professional sounding industries/niches
                    "contact_name": fake.name(),
                    "contact_email": fake.unique.company_email(),
                    "contact_phone": fake.phone_number(),  # Matches 'TEXT' type in Postgres
                    "lead_source": random.choice(self.sources),
                    "estimated_annual_revenue": float(random.randint(50000, 1000000)),  # B2B scale
                    "country_code": random.choice(self.iso_codes),
                    "status": status,
                    "status_updated_at": status_updated_at,
                    "last_activity_at": last_activity_at,
                }
            )

        # 1. Create Polars DataFrame
        df = pl.DataFrame(leads)

        # 2. Path Setup (Using your existing utility structure)
        filename = f"b2b_leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = storage.get_path(bucket, "marketing", filename)

        # 3. Write to Storage
        try:
            # We convert to bytes for compatibility with storage.open 'wb' mode
            csv_data = df.write_csv().encode("utf-8")

            with storage.open(full_path, mode="wb") as f:
                f.write(csv_data)

            logger.info(f"Successfully saved leads to: {full_path}")
        except Exception as e:
            logger.error(f"Failed to save marketing leads: {e}")


if __name__ == "__main__":
    gen = MarketingLeadsGenerator()
    gen.generate()
