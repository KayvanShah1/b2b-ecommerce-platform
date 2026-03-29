import random
from datetime import datetime

import polars as pl
from b2b_ec_utils import get_logger, settings, timed_run
from b2b_ec_utils.storage import storage
from faker import Faker

from b2b_ec_sources import get_connection, get_iso_data

fake = Faker()
logger = get_logger("MarketingLeadsGen")


class MarketingLeadsGenerator:
    def __init__(self):
        # B2B specific sources and statuses
        self.sources = ["LinkedIn Ads", "Google Search", "Trade Show 2026", "Webinar", "Cold Call", "Referral"]
        self.statuses = ["New", "Contacted", "Qualified", "Lost", "Nurturing"]

        # Pull real ISO codes to ensure CSV joins perfectly with Postgres ref_countries
        try:
            self.iso_codes = [c["code"] for c in get_iso_data()]
        except Exception:
            # Fallback if DB is not initialized
            self.iso_codes = ["US", "IN", "QA", "AE", "GB", "DE"]

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

    @timed_run
    def generate(self, count=5000):
        """Generates a B2B marketing leads CSV with Company-level data."""
        logger.info(f"Generating {count} B2B marketing leads via Polars...")

        existing_companies = self.get_existing_companies()
        leads = []

        for _ in range(count):
            # 30% chance the lead is an existing company we are upselling/nurturing
            if existing_companies and random.random() < 0.30:
                company_name = random.choice(existing_companies)
                is_new_entity = False
                status = "Nurturing"
            else:
                # 70% chance it's a brand new B2B prospect
                company_name = fake.company()
                is_new_entity = True
                status = random.choice(["New", "Contacted", "Qualified"])

            leads.append(
                {
                    "lead_id": fake.uuid4(),
                    "created_at": fake.date_time_between(start_date="-1y", end_date="now").strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "company_name": company_name,
                    "is_prospect": is_new_entity,
                    "industry": fake.bs().capitalize(),  # Generates professional sounding industries/niches
                    "contact_name": fake.name(),
                    "contact_email": fake.unique.company_email(),
                    "contact_phone": fake.phone_number(),  # Matches 'TEXT' type in Postgres
                    "lead_source": random.choice(self.sources),
                    "estimated_annual_revenue": float(random.randint(50000, 1000000)),  # B2B scale
                    "country_code": random.choice(self.iso_codes),
                    "status": status,
                }
            )

        # 1. Create Polars DataFrame
        df = pl.DataFrame(leads)

        # 2. Path Setup (Using your existing utility structure)
        bucket = settings.storage.marketing_leads_bucket
        filename = f"b2b_leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = storage.get_path(bucket, "marketing", filename)

        # 3. Write to Storage
        try:
            # We convert to bytes for compatibility with storage.open 'wb' mode
            csv_data = df.write_csv().encode("utf-8")

            with storage.open(full_path, mode="wb") as f:
                f.write(csv_data)

            logger.info(f"Successfully saved {len(df)} B2B leads to: {full_path}")
        except Exception as e:
            logger.error(f"Failed to save marketing leads: {e}")


if __name__ == "__main__":
    gen = MarketingLeadsGenerator()
    gen.generate()
