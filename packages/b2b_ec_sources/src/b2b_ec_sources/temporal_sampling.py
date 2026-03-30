import random
from datetime import datetime, timedelta
from typing import Sequence

import numpy as np


def ordered_bounds(a: float | int, b: float | int):
    return (a, b) if a <= b else (b, a)


def normalize_probabilities(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values.astype(float), 0.0, None)
    total = float(clipped.sum())
    if total <= 0:
        return np.full(len(clipped), 1.0 / len(clipped))
    return clipped / total


def add_months(ts: datetime, months: int) -> datetime:
    month_idx = (ts.month - 1) + months
    year = ts.year + (month_idx // 12)
    month = (month_idx % 12) + 1
    return datetime(year, month, 1)


def _resolve_month_weights(base_month_weights: Sequence[float]) -> np.ndarray:
    if len(base_month_weights) != 12:
        return np.ones(12, dtype=float)
    return np.array(base_month_weights, dtype=float)


def build_month_probability_vector(
    base_month_weights: Sequence[float],
    seasonality_amplitude: float,
    seasonality_peak_month: int,
    month_jitter_sigma: float = 0.0,
) -> np.ndarray:
    months = np.arange(1, 13, dtype=float)
    base_weights = _resolve_month_weights(base_month_weights)
    amplitude = float(np.clip(seasonality_amplitude, 0.0, 0.95))
    peak_month = int(((seasonality_peak_month - 1) % 12) + 1)
    seasonal_curve = 1.0 + amplitude * np.cos((2.0 * np.pi * (months - peak_month)) / 12.0)
    seasonal_curve = np.clip(seasonal_curve, 0.05, None)
    jitter_sigma = max(0.0, float(month_jitter_sigma))
    month_jitter = np.random.lognormal(mean=0.0, sigma=jitter_sigma, size=12)
    raw_scores = base_weights * seasonal_curve * month_jitter
    return normalize_probabilities(raw_scores)


def build_month_seasonality_factors(
    base_month_weights: Sequence[float],
    seasonality_amplitude: float,
    seasonality_peak_month: int,
) -> np.ndarray:
    months = np.arange(1, 13, dtype=float)
    base_weights = _resolve_month_weights(base_month_weights)
    amplitude = float(np.clip(seasonality_amplitude, 0.0, 0.95))
    peak_month = int(((seasonality_peak_month - 1) % 12) + 1)
    seasonal_curve = 1.0 + amplitude * np.cos((2.0 * np.pi * (months - peak_month)) / 12.0)
    seasonal_curve = np.clip(seasonal_curve, 0.05, None)
    raw_scores = np.clip(base_weights * seasonal_curve, 0.05, None)
    return raw_scores / float(np.mean(raw_scores))


def sample_seasonal_volume(
    min_count: int,
    max_count: int,
    now_ts: datetime,
    base_month_weights: Sequence[float],
    seasonality_amplitude: float,
    seasonality_peak_month: int,
    volume_jitter_sigma: float = 0.0,
    min_factor: float = 1.0,
    max_factor: float = 1.0,
) -> int:
    low, high = ordered_bounds(int(min_count), int(max_count))
    base_count = random.randint(max(1, low), max(1, high))
    month_factor = float(
        build_month_seasonality_factors(base_month_weights, seasonality_amplitude, seasonality_peak_month)[
            now_ts.month - 1
        ]
    )
    jitter_sigma = max(0.0, float(volume_jitter_sigma))
    daily_jitter = float(np.random.lognormal(mean=0.0, sigma=jitter_sigma))
    raw_volume = int(round(base_count * month_factor * daily_jitter))

    factor_low, factor_high = ordered_bounds(float(min_factor), float(max_factor))
    lower_bound = max(1, int(round(low * factor_low)))
    upper_bound = max(lower_bound, int(round(high * factor_high)))
    return int(np.clip(raw_volume, lower_bound, upper_bound))


def sample_timestamp_within_window(
    window_start: datetime,
    window_end: datetime,
    month_probs: np.ndarray,
    intra_month_skew_alpha: float,
    intra_month_skew_beta: float,
    day_jitter_std: float,
    hour_jitter_std: float,
    clamp_jitter_to_bucket: bool = True,
) -> datetime:
    if window_start >= window_end:
        return window_end

    month_cursor = datetime(window_start.year, window_start.month, 1)
    month_starts = []
    while month_cursor <= window_end:
        month_starts.append(month_cursor)
        month_cursor = add_months(month_cursor, 1)
    if not month_starts:
        return window_start

    bucket_scores = np.array([month_probs[m.month - 1] for m in month_starts], dtype=float)
    bucket_probs = normalize_probabilities(bucket_scores)
    bucket_idx = int(np.random.choice(len(month_starts), p=bucket_probs))

    bucket_start = max(month_starts[bucket_idx], window_start)
    bucket_end = min(add_months(month_starts[bucket_idx], 1), window_end)
    if bucket_end <= bucket_start:
        return bucket_start

    alpha = max(float(intra_month_skew_alpha), 0.05)
    beta = max(float(intra_month_skew_beta), 0.05)
    within_month_pos = float(np.random.beta(alpha, beta))
    span_seconds = (bucket_end - bucket_start).total_seconds()
    sampled_ts = bucket_start + timedelta(seconds=span_seconds * within_month_pos)

    sampled_ts = sampled_ts + timedelta(
        days=float(np.random.normal(0.0, float(day_jitter_std))),
        hours=float(np.random.normal(0.0, float(hour_jitter_std))),
    )

    if clamp_jitter_to_bucket:
        max_ts = bucket_end - timedelta(microseconds=1)
        if max_ts < bucket_start:
            max_ts = bucket_start
        return min(max(sampled_ts, bucket_start), max_ts)

    max_ts = window_end - timedelta(microseconds=1)
    if max_ts < window_start:
        max_ts = window_start
    return min(max(sampled_ts, window_start), max_ts)
