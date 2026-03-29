import random
from datetime import datetime

import polars as pl
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.settings import settings
from b2b_ec_utils.storage import storage
from b2b_ec_utils.timer import timed_run
from faker import Faker

from b2b_ec_sources import get_connection

fake = Faker()
logger = get_logger("MarketingLeadsGen")


class MarketingLeadsGenerator:
    def __init__(self):
        self.sources = ["LinkedIn Ads", "Google Search", "Trade Show 2025", "Webinar", "Cold Call", "Referral"]
        self.statuses = ["New", "Contacted", "Qualified", "Lost", "Nurturing"]

    def get_existing_companies(self):
        """Fetch real companies from Postgres to create 'Converted' leads."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM companies LIMIT 200")
                return [r[0] for r in cur.fetchall()]
        except Exception as e:
            logger.warning(f"Could not fetch companies from DB: {e}. Using random names instead.")
            return []
        finally:
            conn.close()

    @timed_run
    def generate(self, count=5000):
        """Generates a marketing leads spreadsheet (CSV) using Polars."""
        logger.info(f"Generating {count} marketing leads via Polars...")

        existing_companies = self.get_existing_companies()
        leads = []

        for _ in range(count):
            # 30% match real DB companies, 70% are prospects
            if existing_companies and random.random() < 0.30:
                company_name = random.choice(existing_companies)
                status = "Qualified"
            else:
                company_name = fake.company()
                status = random.choice(self.statuses)

            leads.append(
                {
                    "lead_id": fake.uuid4(),
                    "created_at": fake.date_time_between(start_date="-1y", end_date="now").strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "company_name": company_name,
                    "contact_name": fake.name(),
                    "contact_email": fake.company_email(),
                    "lead_source": random.choice(self.sources),
                    "estimated_budget": float(random.randint(1000, 50000)),
                    "region": random.choice(["North America", "EMEA", "APAC", "LATAM"]),
                    "status": status,
                }
            )

        # 1. Create Polars DataFrame
        df = pl.DataFrame(leads)

        # 2. Path Setup
        bucket = settings.storage.marketing_leads_bucket
        filename = f"leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = storage.get_path(bucket, "marketing", filename)

        # 3. Write to Storage
        # Polars write_csv returns a string if no destination is provided, or we can use bytes
        try:
            # We convert to bytes to maintain compatibility with 'wb' mode in storage.open
            csv_data = df.write_csv().encode("utf-8")

            with storage.open(full_path, mode="wb") as f:
                f.write(csv_data)

            logger.info(f"Successfully saved {len(df)} leads to: {full_path}")
        except Exception as e:
            logger.error(f"Failed to save marketing leads: {e}")


if __name__ == "__main__":
    gen = MarketingLeadsGenerator()
    gen.generate()
