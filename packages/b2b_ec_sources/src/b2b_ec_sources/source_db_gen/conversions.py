"""Lead-conversion logic extracted from the relational source generator."""

import random
from datetime import datetime, timedelta
from typing import Any, Callable

import polars as pl
from b2b_ec_utils.storage import storage

from b2b_ec_sources import coerce_bool
from b2b_ec_sources.geography import sample_country_code
from b2b_ec_sources.source_db_gen.config import CSDGParameters


def _latest_marketing_leads_path() -> str | None:
    pattern = storage.get_marketing_leads_path("b2b_leads_*.csv")
    files = sorted(storage.glob(pattern))
    return files[-1] if files else None


def _load_latest_marketing_leads(logger) -> tuple[pl.DataFrame | None, str | None]:
    latest_path = _latest_marketing_leads_path()
    if not latest_path:
        return None, None

    try:
        with storage.open(latest_path, mode="rb") as file_handle:
            return pl.read_csv(file_handle), latest_path
    except Exception as exc:
        logger.warning(f"LEAD CONVERSION SKIP: could not read {latest_path}: {exc}")
        return None, latest_path


def _parse_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace("T", " ")
    if normalized.endswith("Z"):
        normalized = normalized[:-1]

    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    except ValueError:
        return None


def _lead_conversion_probability(row: dict, now_ts: datetime, params: CSDGParameters) -> float:
    status = str(row.get("status") or "").strip().lower()
    base_by_status = {
        "new": params.lead_conversion_base_new,
        "contacted": params.lead_conversion_base_contacted,
        "nurturing": params.lead_conversion_base_nurturing,
        "qualified": params.lead_conversion_base_qualified,
    }
    base_prob = base_by_status.get(status, 0.0)
    if base_prob <= 0.0:
        return 0.0

    source = str(row.get("lead_source") or "").strip().lower()
    source_multiplier = {
        "referral": 1.35,
        "webinar": 1.20,
        "trade show 2026": 1.15,
        "linkedin ads": 1.00,
        "google search": 1.00,
        "cold call": 0.80,
    }.get(source, 1.00)

    revenue_raw = row.get("estimated_annual_revenue")
    try:
        revenue = max(0.0, float(revenue_raw or 0.0))
    except (TypeError, ValueError):
        revenue = 0.0
    revenue_multiplier = 0.8 + min(1.0, revenue / 1_000_000.0) * 0.6

    anchor_ts = _parse_datetime(row.get("status_updated_at")) or _parse_datetime(row.get("created_at"))
    if anchor_ts is None:
        age_multiplier = 1.0
    else:
        age_days = max(0, (now_ts - anchor_ts).days)
        if age_days <= 3:
            age_multiplier = 1.15
        elif age_days <= 7:
            age_multiplier = 1.30
        elif age_days <= 14:
            age_multiplier = 0.85
        else:
            age_multiplier = 0.55

    noise = 1.0 + random.gauss(0.0, params.lead_conversion_noise_sigma)
    noise = min(1.5, max(0.5, noise))

    probability = base_prob * source_multiplier * revenue_multiplier * age_multiplier * noise
    return min(params.lead_conversion_max_prob, max(params.lead_conversion_min_prob, probability))


def _customer_from_lead(
    *,
    lead_row: dict,
    company_cuit: str,
    country_code: str | None,
    generate_customer_data: Callable[..., dict],
    fake,
) -> dict:
    customer = generate_customer_data(
        company_cuit=company_cuit,
        company_country_code=country_code,
        backdate_from="-30d",
    )

    contact_name = str(lead_row.get("contact_name") or "").strip()
    if contact_name:
        name_parts = [part for part in contact_name.replace(",", " ").split() if part]
        if len(name_parts) == 1:
            customer["first_name"] = name_parts[0]
            customer["last_name"] = fake.last_name()
        elif len(name_parts) >= 2:
            customer["first_name"] = name_parts[0]
            customer["last_name"] = " ".join(name_parts[1:])

    contact_phone = str(lead_row.get("contact_phone") or "").strip()
    if contact_phone:
        customer["phone_number"] = contact_phone

    return customer


def apply_lead_conversions(
    *,
    conn,
    cur,
    params: CSDGParameters,
    stats: dict,
    logger,
    fake,
    client_cuits: list[str],
    company_country_by_cuit: dict[str, str | None],
    client_cuit_by_name: dict[str, str | None],
    country_distribution: dict[str, Any],
    fetch_products_with_supplier_country: Callable[..., list[dict]],
    build_catalog_entries_for_client: Callable[..., list[dict]],
    pg_bulk_copy_rows: Callable[..., None],
    generate_customer_data: Callable[..., dict],
) -> None:
    """Convert a sampled subset of prospect leads into client companies/customers."""
    if not params.enable_lead_conversion:
        return

    leads_df, source_file = _load_latest_marketing_leads(logger)
    if leads_df is None or leads_df.is_empty():
        logger.info("LEAD CONVERSION SKIP: no marketing leads snapshot found.")
        return

    if "lead_id" not in leads_df.columns:
        logger.warning("LEAD CONVERSION SKIP: lead_id column missing in latest leads snapshot.")
        return

    now_ts = datetime.now()
    cutoff_ts = now_ts - timedelta(days=max(1, int(params.lead_conversion_window_days)))

    candidate_rows = []
    for row in leads_df.to_dicts():
        lead_id = str(row.get("lead_id") or "").strip()
        company_name = str(row.get("company_name") or "").strip()
        status = str(row.get("status") or "").strip()
        if not lead_id or not company_name:
            continue
        if status.lower() == "lost":
            continue
        if not coerce_bool(row.get("is_prospect")):
            continue

        activity_ts = _parse_datetime(row.get("status_updated_at")) or _parse_datetime(row.get("created_at"))
        if activity_ts and activity_ts < cutoff_ts:
            continue

        candidate_rows.append(row)

    if params.lead_conversion_max_candidates > 0 and len(candidate_rows) > params.lead_conversion_max_candidates:
        candidate_rows = random.sample(candidate_rows, params.lead_conversion_max_candidates)

    if not candidate_rows:
        logger.info("LEAD CONVERSION SKIP: no eligible prospect leads in conversion window.")
        return

    cur.execute("SELECT lead_id FROM lead_conversions")
    converted_ids = {row[0] for row in cur.fetchall()}

    cur.execute("SELECT lower(email) FROM customers")
    existing_emails = {row[0] for row in cur.fetchall() if row[0]}

    cur.execute("SELECT code FROM ref_countries")
    valid_country_codes = {row[0] for row in cur.fetchall() if row[0]}

    product_rows = fetch_products_with_supplier_country(cur, company_country_by_cuit)

    conversion_rows = []
    new_customer_rows = []
    converted_count = 0
    new_client_count = 0
    new_customer_count = 0

    for lead in candidate_rows:
        lead_id = str(lead.get("lead_id") or "").strip()
        if lead_id in converted_ids:
            continue

        conversion_probability = _lead_conversion_probability(lead, now_ts, params)
        if conversion_probability <= 0 or random.random() > conversion_probability:
            continue

        company_name = str(lead.get("company_name") or "").strip()
        company_key = company_name.lower()
        if company_key in client_cuit_by_name and client_cuit_by_name[company_key] is None:
            # Name collision across multiple client companies; skip ambiguous mapping.
            continue
        country_code = str(lead.get("country_code") or "").strip().upper() or sample_country_code(country_distribution)
        if country_code not in valid_country_codes:
            country_code = sample_country_code(country_distribution)

        company_cuit = client_cuit_by_name.get(company_key)
        if company_cuit:
            country_code = company_country_by_cuit.get(company_cuit) or country_code
        if not company_cuit:
            company_cuit = fake.unique.bothify("##-########-#")
            cur.execute(
                """
                INSERT INTO companies (cuit, name, type, country_code, created_at, updated_at)
                VALUES (%s, %s, 'Client', %s, %s, %s)
                """,
                (company_cuit, company_name, country_code, now_ts, now_ts),
            )
            company_country_by_cuit[company_cuit] = country_code
            existing_cuit = client_cuit_by_name.get(company_key)
            if existing_cuit is None and company_key in client_cuit_by_name:
                # Keep ambiguity marker if name already collided.
                pass
            elif existing_cuit and existing_cuit != company_cuit:
                client_cuit_by_name[company_key] = None
            else:
                client_cuit_by_name[company_key] = company_cuit
            client_cuits.append(company_cuit)
            new_client_count += 1

            if product_rows:
                catalog_rows = build_catalog_entries_for_client(
                    client_cuit=company_cuit,
                    client_country_code=country_code,
                    product_rows=product_rows,
                    now_ts=now_ts,
                )
                pg_bulk_copy_rows(conn, catalog_rows, "company_catalogs")

        if random.random() <= params.lead_conversion_bootstrap_customer_ratio:
            customer_row = _customer_from_lead(
                lead_row=lead,
                company_cuit=company_cuit,
                country_code=country_code,
                generate_customer_data=generate_customer_data,
                fake=fake,
            )
            lead_email = str(lead.get("contact_email") or "").strip().lower()
            if lead_email and lead_email not in existing_emails:
                customer_row["email"] = lead_email
                existing_emails.add(lead_email)
            new_customer_rows.append(customer_row)
            new_customer_count += 1

        conversion_rows.append(
            {
                "lead_id": lead_id,
                "company_cuit": company_cuit,
                "company_name": company_name,
                "country_code": country_code,
                "lead_status": str(lead.get("status") or "").strip() or None,
                "lead_source": str(lead.get("lead_source") or "").strip() or None,
                "conversion_probability": round(float(conversion_probability), 6),
                "converted_at": now_ts,
                "source_file": source_file or None,
                "conversion_window_days": int(params.lead_conversion_window_days),
                "created_at": now_ts,
            }
        )
        converted_ids.add(lead_id)
        converted_count += 1

    if new_customer_rows:
        pg_bulk_copy_rows(conn, new_customer_rows, "customers")
    if conversion_rows:
        pg_bulk_copy_rows(conn, conversion_rows, "lead_conversions")

    stats["lead_conv"] = stats.get("lead_conv", 0) + converted_count
    stats["lead_conv_new_clients"] = stats.get("lead_conv_new_clients", 0) + new_client_count
    stats["lead_conv_new_customers"] = stats.get("lead_conv_new_customers", 0) + new_customer_count

    logger.info(
        f"LEAD CONVERSION: candidates={len(candidate_rows)} converted={converted_count} "
        f"new_clients={new_client_count} bootstrap_customers={new_customer_count}"
    )
