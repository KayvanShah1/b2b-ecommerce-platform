{{ config(materialized='incremental', unique_key='lead_id') }}

with leads as (
    select * from {{ ref('stg_marketing_leads_current') }}
    {% if is_incremental() %}
      where coalesce(_ingested_at, status_updated_at, created_at, cast('1900-01-01' as timestamp)) >= (
        select coalesce(max(coalesce(_ingested_at, status_updated_at, created_at)), cast('1900-01-01' as timestamp))
        from {{ this }}
      )
    {% endif %}
),
companies as (
    select company_cuit, company_name, company_type
    from {{ ref('dim_companies') }}
)
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
