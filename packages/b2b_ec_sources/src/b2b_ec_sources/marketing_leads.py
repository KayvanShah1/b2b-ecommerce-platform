"""Marketing lead generator with temporal behavior and geography-aware contact synthesis."""

import random
from datetime import datetime

import polars as pl
from b2b_ec_utils import get_logger, timed_run
from b2b_ec_utils.storage import storage
from faker import Faker

from b2b_ec_sources import coerce_bool, get_connection
from b2b_ec_sources.geography import (
    build_country_distribution,
    localized_company_name,
    localized_full_name,
    localized_phone,
    sample_country_code,
)
from b2b_ec_sources.lead_gen import (
    MLG,
    MarketingLeadRow,
    MarketingLeadsParameters,
    advance_status,
    index_existing_clients,
    load_previous_leads,
    new_lead_status,
    random_created_at,
    suggest_count,
)
from b2b_ec_sources.temporal_sampling import build_month_probability_vector

fake = Faker()
logger = get_logger("MarketingLeadsGen")


class MarketingLeadsGenerator:
    def __init__(self, params: MarketingLeadsParameters = MLG):
        self.params = params
        self.sources = params.sources
        self.statuses = params.statuses

    def get_existing_companies(self):
        """Fetch existing client companies and countries for aligned lead geography."""
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name, country_code FROM companies WHERE type = 'Client'")
                return [{"name": row[0], "country_code": row[1]} for row in cur.fetchall()]
        except Exception as exc:
            logger.warning(f"Could not fetch companies from DB: {exc}. Using random names.")
            return []
        finally:
            conn.close()

    @timed_run
    def generate(self, count: int | None = None):
        """Generate a B2B marketing leads CSV with company-level data."""
        # Step 1: detect run mode and initialize temporal/geography samplers for this execution.
        prev_df = load_previous_leads(logger=logger)
        is_seed = prev_df is None or prev_df.is_empty()
        now_ts = datetime.now()
        month_probs = build_month_probability_vector(
            base_month_weights=self.params.base_month_weights,
            seasonality_amplitude=self.params.seasonality_amplitude,
            seasonality_peak_month=self.params.seasonality_peak_month,
            month_jitter_sigma=self.params.month_jitter_sigma,
        )
        country_distribution = build_country_distribution()

        # Step 2: estimate target row count from source-company context and prior run volume.
        existing_companies = self.get_existing_companies()
        existing_client_by_name = index_existing_clients(existing_companies)
        prev_count = 0 if prev_df is None else prev_df.height
        target_count = count or suggest_count(
            params=self.params,
            client_count=len(existing_companies),
            is_seed=is_seed,
            prev_count=prev_count,
            now_ts=now_ts,
        )

        logger.info(f"Generating {target_count} B2B marketing leads ({'SEED' if is_seed else 'DAILY'})")
        leads: list[MarketingLeadRow] = []

        # Step 3: carry over and progress a subset of prior leads to preserve CRM continuity.
        carryover_count = 0
        if not is_seed and prev_df is not None and not prev_df.is_empty():
            carryover_count = min(int(target_count * self.params.carryover_ratio), prev_df.height)
            sample_df = prev_df.sample(n=carryover_count, with_replacement=False)
            for row in sample_df.to_dicts():
                current_status = str(row.get("status", "New"))
                next_status = advance_status(current_status)
                status_changed = next_status != current_status

                company_name = str(row.get("company_name") or localized_company_name(row.get("country_code"))).strip()
                company_key = company_name.lower()
                company_meta = existing_client_by_name.get(company_key)
                converted_company = bool(company_meta)
                row_is_prospect = coerce_bool(row.get("is_prospect", False))
                is_prospect = False if converted_company else row_is_prospect

                # If a prior prospect now maps to an actual client company, mark the lifecycle transition explicitly.
                if converted_company and row_is_prospect:
                    next_status = "Converted"
                    status_changed = next_status != current_status
                elif converted_company and next_status not in {"Qualified", "Lost", "Converted"}:
                    next_status = "Qualified"
                    status_changed = next_status != current_status

                # Keep lead geography in sync with canonical company geography after conversion.
                canonical_country_code = company_meta.get("country_code") if company_meta else None
                country_code = (
                    canonical_country_code or row.get("country_code") or sample_country_code(country_distribution)
                )
                canonical_company_name = company_meta.get("name") if company_meta else company_name

                created_at = row.get("created_at") or random_created_at(
                    params=self.params,
                    is_seed=True,
                    now_ts=now_ts,
                    month_probs=month_probs,
                ).strftime("%Y-%m-%d %H:%M:%S")
                status_updated_at = (
                    now_ts.strftime("%Y-%m-%d %H:%M:%S") if status_changed else row.get("status_updated_at")
                )
                status_updated_at = status_updated_at or None
                last_activity_at = status_updated_at or row.get("last_activity_at") or created_at

                leads.append(
                    MarketingLeadRow(
                        lead_id=row.get("lead_id") or fake.uuid4(),
                        created_at=created_at,
                        company_name=canonical_company_name,
                        is_prospect=is_prospect,
                        industry=row.get("industry") or fake.bs().capitalize(),
                        contact_name=row.get("contact_name") or localized_full_name(country_code),
                        contact_email=row.get("contact_email") or fake.unique.company_email(),
                        contact_phone=row.get("contact_phone") or localized_phone(country_code),
                        lead_source=row.get("lead_source") or random.choice(self.sources),
                        estimated_annual_revenue=float(
                            row.get("estimated_annual_revenue") or random.randint(50000, 1000000)
                        ),
                        country_code=country_code,
                        status=next_status,
                        status_updated_at=status_updated_at,
                        last_activity_at=last_activity_at,
                    )
                )

        # Step 4: synthesize new leads for the remaining quota.
        new_count = target_count - carryover_count
        for _ in range(new_count):
            use_existing_company = bool(existing_companies) and random.random() < self.params.existing_company_ratio
            if use_existing_company:
                company_meta = random.choice(existing_companies)
                country_code = company_meta.get("country_code") or sample_country_code(country_distribution)
                company_name = company_meta.get("name") or localized_company_name(country_code)
                is_prospect = False
            else:
                # New B2B prospect
                country_code = sample_country_code(country_distribution)
                company_name = localized_company_name(country_code)
                is_prospect = True

            created_at_dt = random_created_at(
                params=self.params,
                is_seed=is_seed,
                now_ts=now_ts,
                month_probs=month_probs,
            )
            created_at = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")
            status = new_lead_status(use_existing_company)
            status_updated_at = created_at
            last_activity_at = created_at

            leads.append(
                MarketingLeadRow(
                    lead_id=fake.uuid4(),
                    created_at=created_at,
                    company_name=company_name,
                    is_prospect=is_prospect,
                    industry=fake.bs().capitalize(),
                    contact_name=localized_full_name(country_code),
                    contact_email=fake.unique.company_email(),
                    contact_phone=localized_phone(country_code),
                    lead_source=random.choice(self.sources),
                    estimated_annual_revenue=float(random.randint(50000, 1000000)),
                    country_code=country_code,
                    status=status,
                    status_updated_at=status_updated_at,
                    last_activity_at=last_activity_at,
                )
            )

        df = pl.DataFrame([lead.model_dump(mode="python") for lead in leads])

        filename = f"b2b_leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        full_path = storage.get_marketing_leads_path(filename)

        try:
            # Step 5: write a single run artifact to object storage.
            csv_data = df.write_csv().encode("utf-8")
            with storage.open(full_path, mode="wb") as file_handle:
                file_handle.write(csv_data)
            logger.info(f"Successfully saved leads to: {full_path}")
        except Exception as exc:
            logger.error(f"Failed to save marketing leads: {exc}")
            raise


if __name__ == "__main__":
    generator = MarketingLeadsGenerator()
    generator.generate()
