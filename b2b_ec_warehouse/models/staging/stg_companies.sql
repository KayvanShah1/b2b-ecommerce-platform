with source as (
    select * from {{ source('ingestion', 'companies') }}
),
renamed as (
    select
        cuit as company_cuit,
        name as company_name,
        type as company_type,
        upper(country_code) as country_code,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
)
select * from renamed
