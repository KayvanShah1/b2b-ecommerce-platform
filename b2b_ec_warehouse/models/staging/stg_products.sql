with source as (
    select * from {{ source('ingestion', 'products') }}
),
renamed as (
    select
        cast(id as bigint) as product_id,
        name as product_name,
        supplier_cuit,
        cast(base_price as double) as base_price,
        cast(base_price as double) as product_price,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(false as boolean) as is_food_item,
        cast(false as boolean) as is_drink_item
    from source
)
select * from renamed
