import random
from datetime import datetime

import polars as pl
from b2b_ec_utils.logger import get_logger
from b2b_ec_utils.settings import settings  # Import settings directly
from b2b_ec_utils.storage import storage
from b2b_ec_utils.timer import timed_run
from faker import Faker

from b2b_ec_sources import get_connection

fake = Faker()
logger = get_logger("WebLogGenerator")


class WebLogGenerator:
    def __init__(self):
        self.ua_pool = {
            "Desktop-Windows-Chrome": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Desktop-Mac-Safari": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Laptop-Linux-Firefox": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Tablet-iPad": "Mozilla/5.0 (iPad; CPU OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
            "Mobile-Android-Samsung": "Mozilla/5.0 (Linux; Android 13; SM-S911B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.111 Mobile Safari/537.36",
            "Mobile-iPhone-App": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 [B2B_Native_App/2.4.1]",
        }
        self.ua_keys = list(self.ua_pool.keys())
        self.ua_weights = [0.40, 0.20, 0.05, 0.05, 0.25, 0.05]

    def get_eligible_users(self, is_seed: bool):
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                query = "SELECT username, created_at FROM customers"
                if not is_seed:
                    query += " WHERE created_at > now() - interval '2 days'"

                cur.execute(query)
                results = cur.fetchall()
                return [{"username": r[0], "created_at": r[1]} for r in results]
        finally:
            conn.close()

    @timed_run
    def generate(self, log_count=None):
        # 1. Identify the target bucket from settings
        log_bucket = settings.storage.webserver_logs_bucket

        # 2. State Detection: Check if 'seed' folder has data using get_path
        seed_glob_pattern = storage.get_path(log_bucket, "seed", "*.log")
        existing_seeds = storage.glob(seed_glob_pattern)

        is_seed = len(existing_seeds) == 0

        if is_seed:
            logger.info(f"No seed logs detected in {log_bucket}. Initializing SEED mode.")
        else:
            logger.info(f"Seed data exists in {log_bucket}. Initializing DAILY mode.")

        # 3. Get User Pool
        users = self.get_eligible_users(is_seed)
        if not users:
            logger.warning("No users found in Database. Please run the Postgres generator first.")
            return

        # 4. Determine Volume (Synced to 100k Orders)
        count = log_count or (100000 if is_seed else random.randint(3000, 6000))
        logs = []

        # 5. Generate Logs
        for _ in range(count):
            user_meta = random.choice(users)
            dt = fake.date_time_between(start_date=user_meta["created_at"], end_date="now")
            ts_str = dt.strftime("%d/%b/%Y:%H:%M:%S +0000")

            ip = fake.ipv4()
            username = user_meta["username"]
            ua_str = self.ua_pool[random.choices(self.ua_keys, weights=self.ua_weights)[0]]

            # Combined Log Format
            line = f'{ip} - {username} [{ts_str}] "GET /api/v1/catalog HTTP/1.1" 200 1500 "-" "{ua_str}"'
            logs.append({"ts": dt, "line": line})

        # 6. Sort and Write to Storage
        df = pl.DataFrame(logs).sort("ts")

        folder = "seed" if is_seed else "daily"
        filename = f"access_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        # Use get_path to build the URI for the specific bucket
        full_path = storage.get_path(log_bucket, folder, filename)

        log_payload = "\n".join(df["line"].to_list()) + "\n"

        # storage.open now handles parent directory creation for local storage automatically
        with storage.open(full_path, mode="wb") as f:
            f.write(log_payload.encode("utf-8"))

        logger.info(f"Successfully generated {len(df)} logs to: {full_path}")


if __name__ == "__main__":
    gen = WebLogGenerator()
    gen.generate()
