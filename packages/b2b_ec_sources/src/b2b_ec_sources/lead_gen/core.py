"""Shared helper functions for marketing-leads generation."""

import random
from datetime import datetime, timedelta

import polars as pl
from b2b_ec_sources.temporal_sampling import sample_seasonal_volume, sample_timestamp_within_window
from b2b_ec_utils.storage import storage

from .config import MarketingLeadsParameters


def list_previous_leads_files() -> list[str]:
    pattern = storage.get_marketing_leads_path("b2b_leads_*.csv")
    files = storage.glob(pattern)
    return sorted(files)


def load_previous_leads(logger) -> pl.DataFrame | None:
    files = list_previous_leads_files()
    if not files:
        return None
    latest = files[-1]
    try:
        with storage.open(latest, mode="rb") as file_handle:
            return pl.read_csv(file_handle)
    except Exception as exc:
        logger.warning(f"Could not read previous leads file {latest}: {exc}")
        return None


def index_existing_clients(existing_companies: list[dict]) -> dict[str, dict[str, str | None]]:
    indexed: dict[str, dict[str, str | None]] = {}
    ambiguous_names: set[str] = set()

    for company in existing_companies:
        raw_name = str(company.get("name") or "").strip()
        if not raw_name:
            continue
        key = raw_name.lower()
        if key in ambiguous_names:
            continue

        meta = {
            "name": raw_name,
            "country_code": str(company.get("country_code") or "").strip().upper() or None,
        }
        if key in indexed:
            indexed.pop(key, None)
            ambiguous_names.add(key)
            continue
        indexed[key] = meta

    return indexed


def advance_status(current_status: str) -> str:
    if current_status in {"Lost", "Converted"}:
        return current_status
    roll = random.random()
    if roll < 0.15:
        return "Lost"
    if roll < 0.45:
        return "Contacted" if current_status == "New" else "Qualified"
    if roll < 0.65:
        return "Nurturing"
    return current_status


def suggest_count(
    params: MarketingLeadsParameters,
    client_count: int,
    is_seed: bool,
    prev_count: int,
    now_ts: datetime,
) -> int:
    if is_seed:
        base = max(params.min_seed_leads, int(client_count * params.seed_leads_per_company))
        return int(base * random.uniform(0.8, 1.2))

    per_company = int(client_count * random.uniform(params.daily_leads_per_company_min, params.daily_leads_per_company_max))
    from_previous = int(prev_count * random.uniform(0.05, 0.15)) if prev_count else 0
    baseline = min(params.max_daily_leads, max(params.min_daily_leads, per_company, from_previous))
    range_min = max(params.min_daily_leads, int(round(baseline * 0.8)))
    range_max = min(params.max_daily_leads, int(round(baseline * 1.2)))
    return sample_seasonal_volume(
        min_count=range_min,
        max_count=max(range_min, range_max),
        now_ts=now_ts,
        base_month_weights=params.base_month_weights,
        seasonality_amplitude=params.seasonality_amplitude,
        seasonality_peak_month=params.seasonality_peak_month,
        volume_jitter_sigma=params.daily_volume_jitter_sigma,
        min_factor=params.min_daily_volume_factor,
        max_factor=params.max_daily_volume_factor,
    )


def new_lead_status(is_existing_company: bool) -> str:
    if is_existing_company:
        return random.choices(["Nurturing", "Qualified", "Contacted"], weights=[0.5, 0.3, 0.2])[0]
    return random.choices(["New", "Contacted", "Qualified"], weights=[0.6, 0.3, 0.1])[0]


def random_created_at(
    params: MarketingLeadsParameters,
    is_seed: bool,
    now_ts: datetime,
    month_probs,
) -> datetime:
    window_days = params.seed_distribution_days if is_seed else params.daily_distribution_days
    window_start = now_ts - timedelta(days=max(1, int(window_days)))
    return sample_timestamp_within_window(
        window_start=window_start,
        window_end=now_ts,
        month_probs=month_probs,
        intra_month_skew_alpha=params.intra_month_skew_alpha,
        intra_month_skew_beta=params.intra_month_skew_beta,
        day_jitter_std=params.day_jitter_std,
        hour_jitter_std=params.hour_jitter_std,
        clamp_jitter_to_bucket=params.clamp_jitter_to_bucket,
    )
