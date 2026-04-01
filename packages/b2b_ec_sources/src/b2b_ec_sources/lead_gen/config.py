"""Configuration for the marketing-leads generator."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MarketingLeadsParameters(BaseSettings):
    sources: list[str] = [
        "LinkedIn Ads",
        "Google Search",
        "Trade Show 2026",
        "Webinar",
        "Cold Call",
        "Referral",
    ]
    statuses: list[str] = ["New", "Contacted", "Qualified", "Lost", "Nurturing", "Converted"]

    existing_company_ratio: float = Field(default=0.70, ge=0.0, le=1.0)
    seed_leads_per_company: int = Field(default=25, ge=1)
    min_seed_leads: int = Field(default=1500, ge=1)

    daily_leads_per_company_min: float = Field(default=2.0, ge=0.0)
    daily_leads_per_company_max: float = Field(default=6.0, ge=0.0)
    min_daily_leads: int = Field(default=100, ge=1)
    max_daily_leads: int = Field(default=950, ge=1)

    seed_distribution_days: int = Field(default=365, ge=1)
    daily_distribution_days: int = Field(default=30, ge=1)
    base_month_weights: list[float] = [1.0] * 12
    seasonality_amplitude: float = Field(default=0.30, ge=0.0, le=0.95)
    seasonality_peak_month: int = Field(default=11, ge=1, le=12)
    month_jitter_sigma: float = Field(default=0.10, ge=0.0)
    intra_month_skew_alpha: float = Field(default=2.2, gt=0.0)
    intra_month_skew_beta: float = Field(default=2.8, gt=0.0)
    day_jitter_std: float = Field(default=1.0, ge=0.0)
    hour_jitter_std: float = Field(default=2.0, ge=0.0)
    clamp_jitter_to_bucket: bool = True
    daily_volume_jitter_sigma: float = Field(default=0.08, ge=0.0)
    min_daily_volume_factor: float = Field(default=0.70, ge=0.0)
    max_daily_volume_factor: float = Field(default=1.40, ge=0.0)

    carryover_ratio: float = Field(default=0.35, ge=0.0, le=1.0)

    model_config = SettingsConfigDict(env_prefix="MARKETING_LEADS_", extra="ignore")

    def model_post_init(self, __context):
        if self.daily_leads_per_company_min > self.daily_leads_per_company_max:
            raise ValueError("daily_leads_per_company_min must be <= daily_leads_per_company_max")
        if self.min_daily_leads > self.max_daily_leads:
            raise ValueError("min_daily_leads must be <= max_daily_leads")
        if self.min_daily_volume_factor > self.max_daily_volume_factor:
            raise ValueError("min_daily_volume_factor must be <= max_daily_volume_factor")
        if len(self.base_month_weights) != 12:
            raise ValueError("base_month_weights must contain 12 values")


MLG = MarketingLeadsParameters()

