with source as (
    select * from {{ source('ingestion', 'company_catalogs') }}
),
renamed as (
    select
        company_cuit,
        cast(product_id as bigint) as product_id,
        cast(sale_price as double) as sale_price,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
)
select * from renamed
