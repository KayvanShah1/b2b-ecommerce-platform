import json
import random
from datetime import datetime

import polars as pl
from b2b_ec_utils import get_logger, settings, timed_run
from b2b_ec_utils.storage import storage
from faker import Faker
from pydantic_settings import BaseSettings

from b2b_ec_sources import get_connection

fake = Faker()
logger = get_logger("WebLogGenerator")


class WLGParameters(BaseSettings):
    SEED_LOG_COUNT: int = 100000
    DAILY_LOG_MIN: int = 5000
    DAILY_LOG_MAX: int = 10000


W = WLGParameters()


class WebLogGenerator:
    def __init__(self):
        # 1. Expanded User Agent Pool (B2B Corporate bias)
        self.ua_pool = {
            "Win-Chrome-120": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Win-Edge-120": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/120.0.0.0",
            "Mac-Safari-17": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mac-Chrome-120": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Linux-Firefox-121": "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "iPhone-15-Safari": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
            "Android-S23-Chrome": "Mozilla/5.0 (Linux; Android 14; SM-S911B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.144 Mobile Safari/537.36",
            "Googlebot": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        }
        self.ua_keys = list(self.ua_pool.keys())
        self.ua_weights = [0.40, 0.15, 0.10, 0.10, 0.05, 0.08, 0.07, 0.05]

        # 2. Weighted Endpoints & Status Codes
        # Format: (Method, Path, [(StatusCode, Weight), ...], GlobalPathWeight)
        self.endpoints = [
            ("GET", "/api/v1/catalog", [(200, 95), (404, 5)], 40),
            ("GET", "/api/v1/products", [(200, 98), (404, 2)], 30),
            ("GET", "/health", [(200, 99), (500, 1)], 15),
            ("POST", "/api/v1/login", [(200, 70), (401, 25), (403, 5)], 10),
            ("POST", "/api/v1/orders", [(201, 85), (400, 10), (401, 5)], 5),
        ]

    def get_eligible_users(self, is_seed: bool):
        """Fetch users to ensure logs only exist for valid DB entities."""
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
        log_bucket = settings.storage.webserver_logs_bucket

        # Check for existing seed data to determine mode
        seed_path = storage.get_path(log_bucket, "seed", "*.jsonl")
        is_seed = len(storage.glob(seed_path)) == 0

        users = self.get_eligible_users(is_seed)
        if not users:
            logger.error("No users found in Postgres. Seed the database first.")
            return

        # Volume: 100k for initial history, ~5k for daily increments
        count = log_count or (W.SEED_LOG_COUNT if is_seed else random.randint(W.DAILY_LOG_MIN, W.DAILY_LOG_MAX))
        logs = []

        logger.info(f"Generating {count} JSONL logs ({'SEED' if is_seed else 'DAILY'})")

        for _ in range(count):
            user_meta = random.choice(users)

            # 10% unauthenticated traffic
            username = "-" if random.random() < 0.10 else user_meta["username"]

            # Timestamp must be after the user's account was created
            dt = fake.date_time_between(start_date=user_meta["created_at"], end_date="now")

            # Weighted selection of endpoint
            ep = random.choices(self.endpoints, weights=[e[3] for e in self.endpoints])[0]
            method, path, status_map = ep[0], ep[1], ep[2]

            # Weighted selection of status code for that specific endpoint
            status = random.choices([s[0] for s in status_map], weights=[s[1] for s in status_map])[0]

            # Select User Agent
            ua = self.ua_pool[random.choices(self.ua_keys, weights=self.ua_weights)[0]]

            # Construct the JSON Record
            log_entry = {
                "remote_host": fake.ipv4(),
                "ident": "-",
                "username": username,
                "timestamp": dt.isoformat(),
                "request_method": method,
                "request_path": path,
                "http_version": "HTTP/1.1",
                "status_code": status,
                "response_size_bytes": random.randint(300, 5000),
                "referer": "https://b2b-platform.com/app" if random.random() > 0.4 else "-",
                "user_agent": ua,
            }
            logs.append({"ts": dt, "data": log_entry})

        # Chronological sorting using Polars
        df = pl.DataFrame(logs).sort("ts")

        folder = "seed" if is_seed else "daily"
        filename = f"access_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        full_path = storage.get_path(log_bucket, folder, filename)

        # Serialize to JSON Lines (JSONL)
        jsonl_payload = "\n".join([json.dumps(row) for row in df["data"].to_list()]) + "\n"

        try:
            with storage.open(full_path, mode="wb") as f:
                f.write(jsonl_payload.encode("utf-8"))
            logger.info(f"Successfully wrote {len(df)} logs to {full_path}")
        except Exception as e:
            logger.error(f"Failed to write logs: {e}")


if __name__ == "__main__":
    gen = WebLogGenerator()
    gen.generate()
