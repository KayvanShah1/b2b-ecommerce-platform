"""Web log generator aligned with source users, temporal patterns, and geography."""

import json
import random
import uuid
from datetime import datetime, timedelta

import polars as pl
from b2b_ec_utils import get_logger, timed_run
from b2b_ec_utils.storage import storage
from pydantic_settings import BaseSettings, SettingsConfigDict

from b2b_ec_sources import get_connection
from b2b_ec_sources.geography import build_country_distribution, sample_country_code, sample_ip_for_country
from b2b_ec_sources.temporal_sampling import (
    build_month_probability_vector,
    sample_seasonal_volume,
    sample_timestamp_within_window,
)

logger = get_logger("WebLogGenerator")


class WLGParameters(BaseSettings):
    SEED_LOG_COUNT: int = 100000
    DAILY_LOG_MIN: int = 5000
    DAILY_LOG_MAX: int = 10000

    SEED_DISTRIBUTION_DAYS: int = 365
    DAILY_DISTRIBUTION_DAYS: int = 30

    BASE_MONTH_WEIGHTS: list[float] = [1.0] * 12
    SEASONALITY_AMPLITUDE: float = 0.30
    SEASONALITY_PEAK_MONTH: int = 11
    MONTH_JITTER_SIGMA: float = 0.10

    INTRA_MONTH_SKEW_ALPHA: float = 2.2
    INTRA_MONTH_SKEW_BETA: float = 2.8
    DAY_JITTER_STD: float = 1.0
    HOUR_JITTER_STD: float = 2.0
    CLAMP_JITTER_TO_BUCKET: bool = True

    DAILY_VOLUME_JITTER_SIGMA: float = 0.06
    MIN_DAILY_VOLUME_FACTOR: float = 0.70
    MAX_DAILY_VOLUME_FACTOR: float = 1.40
    ENFORCE_HARD_DAILY_MIN: bool = True

    UNAUTH_TRAFFIC_RATIO: float = 0.10
    NEW_USER_LOOKBACK_DAYS: int = 2
    AUTH_RETURNING_USER_RATIO: float = 0.75
    AUTH_NEW_USER_RATIO: float = 0.25

    model_config = SettingsConfigDict(env_prefix="WEB_LOGS_", extra="ignore")

    def model_post_init(self, __context):
        if self.DAILY_LOG_MIN > self.DAILY_LOG_MAX:
            raise ValueError("DAILY_LOG_MIN must be <= DAILY_LOG_MAX")
        if self.MIN_DAILY_VOLUME_FACTOR > self.MAX_DAILY_VOLUME_FACTOR:
            raise ValueError("MIN_DAILY_VOLUME_FACTOR must be <= MAX_DAILY_VOLUME_FACTOR")
        if len(self.BASE_MONTH_WEIGHTS) != 12:
            raise ValueError("BASE_MONTH_WEIGHTS must contain 12 values")
        if not 0.0 <= self.UNAUTH_TRAFFIC_RATIO <= 1.0:
            raise ValueError("UNAUTH_TRAFFIC_RATIO must be between 0 and 1")
        if self.NEW_USER_LOOKBACK_DAYS < 1:
            raise ValueError("NEW_USER_LOOKBACK_DAYS must be >= 1")
        if self.AUTH_RETURNING_USER_RATIO < 0 or self.AUTH_NEW_USER_RATIO < 0:
            raise ValueError("AUTH_RETURNING_USER_RATIO and AUTH_NEW_USER_RATIO must be >= 0")
        if self.AUTH_RETURNING_USER_RATIO + self.AUTH_NEW_USER_RATIO <= 0:
            raise ValueError("AUTH_RETURNING_USER_RATIO + AUTH_NEW_USER_RATIO must be > 0")


W = WLGParameters()


class WebLogGenerator:
    def __init__(self):
        # Corporate-heavy user-agent mix to resemble B2B traffic.
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

        # Format: (method, path, [(status_code, weight), ...], path_weight)
        self.endpoints = [
            ("GET", "/api/v1/catalog", [(200, 95), (404, 5)], 40),
            ("GET", "/api/v1/products", [(200, 98), (404, 2)], 30),
            ("GET", "/health", [(200, 99), (500, 1)], 15),
            ("POST", "/api/v1/login", [(200, 70), (401, 25), (403, 5)], 10),
            ("POST", "/api/v1/orders", [(201, 85), (400, 10), (401, 5)], 5),
        ]

    def get_eligible_users(self):
        """Fetch users to ensure logs only exist for valid DB entities."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                query = """
                    SELECT cust.username, cust.created_at, comp.country_code
                    FROM customers AS cust
                    LEFT JOIN companies AS comp
                      ON cust.company_cuit = comp.cuit
                """
                cur.execute(query)
                results = cur.fetchall()
                return [{"username": r[0], "created_at": r[1], "country_code": r[2]} for r in results]
        finally:
            conn.close()

    def _partition_users(self, users: list[dict], now_ts: datetime):
        # Split users into recent-vs-returning cohorts for daily traffic mixing.
        cutoff = now_ts - timedelta(days=W.NEW_USER_LOOKBACK_DAYS)
        recent, returning = [], []
        for u in users:
            created_at = u.get("created_at")
            if isinstance(created_at, datetime) and created_at >= cutoff:
                recent.append(u)
            else:
                returning.append(u)
        return returning, recent

    def _pick_user(self, users: list[dict], returning_users: list[dict], recent_users: list[dict], is_seed: bool):
        if is_seed:
            # Seed mode does not enforce cohort bias; sample full user population.
            return random.choice(users)

        # Daily mode favors returning users but still injects new-user traffic.
        if returning_users and recent_users:
            total_weight = W.AUTH_RETURNING_USER_RATIO + W.AUTH_NEW_USER_RATIO
            p_returning = W.AUTH_RETURNING_USER_RATIO / total_weight
            pool = returning_users if random.random() < p_returning else recent_users
            return random.choice(pool)

        if returning_users:
            return random.choice(returning_users)

        if recent_users:
            return random.choice(recent_users)

        return random.choice(users)

    @timed_run
    def generate(self, log_count=None):
        # Step 1: establish run mode and initialize temporal/geography samplers.
        # Decide between one-time seed generation and daily incremental logs.
        seed_path = storage.get_webserver_logs_path(True, "*.jsonl")
        is_seed = len(storage.glob(seed_path)) == 0
        now_ts = datetime.now()
        month_probs = build_month_probability_vector(
            base_month_weights=W.BASE_MONTH_WEIGHTS,
            seasonality_amplitude=W.SEASONALITY_AMPLITUDE,
            seasonality_peak_month=W.SEASONALITY_PEAK_MONTH,
            month_jitter_sigma=W.MONTH_JITTER_SIGMA,
        )
        country_distribution = build_country_distribution()

        users = self.get_eligible_users()
        if not users:
            logger.error("No users found in Postgres. Seed the database first.")
            return
        # Step 2: derive user cohorts used for daily authenticated traffic selection.
        returning_users, recent_users = self._partition_users(users, now_ts)

        # Step 3: decide output volume.
        # Seed uses a large baseline; daily runs use seasonal volume sampling.
        if log_count is not None:
            count = log_count
        elif is_seed:
            count = W.SEED_LOG_COUNT
        else:
            sampled_count = sample_seasonal_volume(
                min_count=W.DAILY_LOG_MIN,
                max_count=W.DAILY_LOG_MAX,
                now_ts=now_ts,
                base_month_weights=W.BASE_MONTH_WEIGHTS,
                seasonality_amplitude=W.SEASONALITY_AMPLITUDE,
                seasonality_peak_month=W.SEASONALITY_PEAK_MONTH,
                volume_jitter_sigma=W.DAILY_VOLUME_JITTER_SIGMA,
                min_factor=W.MIN_DAILY_VOLUME_FACTOR,
                max_factor=W.MAX_DAILY_VOLUME_FACTOR,
            )
            count = max(W.DAILY_LOG_MIN, sampled_count) if W.ENFORCE_HARD_DAILY_MIN else sampled_count
        logs = []

        logger.info(f"Generating {count} JSONL logs ({'SEED' if is_seed else 'DAILY'})")

        # Step 4: synthesize events with consistent user/time/endpoint relationships.
        for _ in range(count):
            user_meta = self._pick_user(users, returning_users, recent_users, is_seed)

            # Keep a configurable unauthenticated traffic share.
            username = "-" if random.random() < W.UNAUTH_TRAFFIC_RATIO else user_meta["username"]
            if username == "-":
                country_code = sample_country_code(country_distribution)
            else:
                country_code = user_meta.get("country_code") or sample_country_code(country_distribution)

            # Ensure event timestamps are not earlier than the user creation time.
            window_days = W.SEED_DISTRIBUTION_DAYS if is_seed else W.DAILY_DISTRIBUTION_DAYS
            window_start = now_ts - timedelta(days=max(1, int(window_days)))
            created_at = user_meta.get("created_at")
            if not isinstance(created_at, datetime):
                created_at = window_start
            effective_start = max(created_at, window_start)
            dt = sample_timestamp_within_window(
                window_start=effective_start,
                window_end=now_ts,
                month_probs=month_probs,
                intra_month_skew_alpha=W.INTRA_MONTH_SKEW_ALPHA,
                intra_month_skew_beta=W.INTRA_MONTH_SKEW_BETA,
                day_jitter_std=W.DAY_JITTER_STD,
                hour_jitter_std=W.HOUR_JITTER_STD,
                clamp_jitter_to_bucket=W.CLAMP_JITTER_TO_BUCKET,
            )

            ep = random.choices(self.endpoints, weights=[e[3] for e in self.endpoints])[0]
            method, path, status_map = ep[0], ep[1], ep[2]

            status = random.choices([s[0] for s in status_map], weights=[s[1] for s in status_map])[0]

            ua = self.ua_pool[random.choices(self.ua_keys, weights=self.ua_weights)[0]]

            log_entry = {
                "event_id": uuid.uuid4().hex,
                "remote_host": sample_ip_for_country(country_code),
                "ident": "-",
                "username": username,
                "country_code": country_code,
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

        df = pl.DataFrame(logs).sort("ts")

        filename = f"access_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        full_path = storage.get_webserver_logs_path(is_seed, filename)

        # Step 5: persist one JSONL artifact for this run.
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
