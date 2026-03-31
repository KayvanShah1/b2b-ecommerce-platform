{{ config(materialized='incremental', unique_key=['lead_id', '_ingestion_run_id']) }}

with leads as (
    select * from {{ ref('stg_marketing_leads_history') }}
    {% if is_incremental() %}
      where coalesce(_ingested_at, created_at, cast('1900-01-01' as timestamp)) > (
        select coalesce(max(coalesce(_ingested_at, created_at)), cast('1900-01-01' as timestamp))
        from {{ this }}
      )
    {% endif %}
)
select
    lead_id,
    company_name,
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
    {# _source_file, #}
    _ingestion_run_id,
    _ingested_at,
    current_timestamp as loaded_at
from leads
