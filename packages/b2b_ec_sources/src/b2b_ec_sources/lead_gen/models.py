"""Typed row models for marketing-leads output."""

from pydantic import BaseModel, field_validator


class MarketingLeadRow(BaseModel):
    lead_id: str
    created_at: str
    company_name: str
    is_prospect: bool
    industry: str
    contact_name: str
    contact_email: str
    contact_phone: str
    lead_source: str
    estimated_annual_revenue: float
    country_code: str
    status: str
    status_updated_at: str | None
    last_activity_at: str

    @field_validator("lead_id", mode="before")
    @classmethod
    def _coerce_lead_id(cls, value):
        return str(value)

    @field_validator("country_code", mode="before")
    @classmethod
    def _normalize_country_code(cls, value):
        text = str(value or "").strip().upper()
        return text

    @field_validator("estimated_annual_revenue", mode="after")
    @classmethod
    def _non_negative_revenue(cls, value: float):
        return max(0.0, float(value))

