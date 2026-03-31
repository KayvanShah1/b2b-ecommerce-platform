with source as (
    select * from {{ source('ingestion', 'marketing_leads_current') }}
),
renamed as (
    select
        lead_id,
        company_name,
        cast(created_at as timestamp) as created_at,
        cast(is_prospect as boolean) as is_prospect,
        industry,
        contact_name,
        contact_email,
        contact_phone,
        lead_source,
        cast(estimated_annual_revenue as double) as estimated_annual_revenue,
        upper(country_code) as country_code,
        status,
        cast(status_updated_at as timestamp) as status_updated_at,
        cast(last_activity_at as timestamp) as last_activity_at,
        _source_file,
        _ingestion_run_id,
        cast(_ingested_at as timestamp) as _ingested_at
    from source
)
select * from renamed
