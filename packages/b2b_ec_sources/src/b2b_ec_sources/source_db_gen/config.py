"""Configuration constants for the relational source generator."""

from pydantic import BaseModel


class CSDGParameters(BaseModel):
    # Initial Seed Volume
    seed_total_companies: int = 100
    seed_supplier_ratio: float = 0.20
    seed_total_customers: int = 10000
    seed_total_products: int = 700
    seed_total_orders: int = 100000
    catalog_size_seed: int = 250

    # Organic Growth (Steady State)
    min_target_cust_growth: float = 0.01
    max_target_cust_growth: float = 0.10
    min_target_prod_growth: float = 0.01
    max_target_prod_growth: float = 0.05

    # Dynamic Evolution Engine
    min_event_slots: int = 1
    max_event_slots: int = 5
    prob_new_company: float = 0.15
    prob_price_update: float = 0.05
    prob_supplier: float = 0.20
    prob_client: float = 0.80

    # Lead-to-client conversion (applied during evolution from latest lead snapshot file)
    enable_lead_conversion: bool = True
    lead_conversion_window_days: int = 7
    lead_conversion_max_candidates: int = 2500
    lead_conversion_base_new: float = 0.002
    lead_conversion_base_contacted: float = 0.008
    lead_conversion_base_nurturing: float = 0.010
    lead_conversion_base_qualified: float = 0.030
    lead_conversion_noise_sigma: float = 0.15
    lead_conversion_min_prob: float = 0.001
    lead_conversion_max_prob: float = 0.200
    lead_conversion_bootstrap_customer_ratio: float = 0.85

    # Financials
    min_markup_percent: float = 1.20
    max_markup_percent: float = 2.50
    min_base_price: float = 10.0
    max_base_price: float = 1000.0

    # Incremental (daily) order volume targets in steady state
    min_inc_orders: int = 100
    max_inc_orders: int = 450
    min_items_per_order: int = 1
    max_items_per_order: int = 6
    min_item_quantity: int = 1
    max_item_quantity: int = 5
    new_supplier_products: int = 20

    # Statuses & Returns
    seed_status_weights: list[float] = [0.93, 0.03, 0.04]  # COMPLETED, CANCELLED, RETURNED
    min_return_rate: float = 0.02
    max_return_rate: float = 0.06

    # Temporal Order Distribution (Monthly + Seasonality + Skew + Jitter)
    seed_distribution_days: int = 365
    incremental_distribution_days: int = 60
    base_month_weights: list[float] = [1.0] * 12
    seasonality_amplitude: float = 0.35
    seasonality_peak_month: int = 11
    month_jitter_sigma: float = 0.20
    incremental_volume_jitter_sigma: float = 0.08
    min_incremental_volume_factor: float = 0.60
    max_incremental_volume_factor: float = 1.60
    intra_month_skew_alpha: float = 2.2
    intra_month_skew_beta: float = 2.8
    day_jitter_std: float = 1.5
    hour_jitter_std: float = 3.0


DEFAULT_CSDG_PARAMS = CSDGParameters()
