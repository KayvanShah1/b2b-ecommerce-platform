{{ config(materialized='incremental', unique_key='lead_id') }}

with leads as (
    select * from {{ ref('stg_marketing_leads_current') }}
),
companies_ranked as (
    select
        company_cuit,
        company_name,
        company_type,
        row_number() over (
            partition by lower(company_name)
            order by
                case when company_type = 'Client' then 0 else 1 end,
                updated_at desc,
                created_at desc,
                company_cuit
        ) as company_name_rank
    from {{ ref('dim_companies') }}
),
companies as (
    select company_cuit, company_name, company_type
    from companies_ranked
    where company_name_rank = 1
),
enriched as (
    select
        leads.lead_id,
        leads.company_name,
        companies.company_cuit,
        companies.company_type,
        leads.is_prospect,
        leads.industry,
        leads.contact_name,
        leads.contact_email,
        leads.contact_phone,
        leads.lead_source,
        leads.estimated_annual_revenue,
        leads.country_code,
        leads.status,
        leads.created_at,
        leads.status_updated_at,
        leads.last_activity_at,
        {# leads._source_file, #}
        leads._ingestion_run_id,
        leads._ingested_at,
        current_timestamp as loaded_at
    from leads
    left join companies
      on lower(leads.company_name) = lower(companies.company_name)
)
select
    lead_id,
    company_name,
    company_cuit,
    company_type,
    is_prospect,
    industry,
    contact_name,
    contact_email,
    contact_phone,
    lead_source,
    estimated_annual_revenue,
    country_code,
    status,
    created_at,
    status_updated_at,
    last_activity_at,
    _ingestion_run_id,
    _ingested_at,
    loaded_at
from enriched
qualify row_number() over (
    partition by lead_id
    order by
        coalesce(_ingested_at, status_updated_at, created_at, cast('1900-01-01' as timestamp)) desc,
        loaded_at desc
) = 1
