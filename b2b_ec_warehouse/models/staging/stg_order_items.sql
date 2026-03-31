with source as (
    select * from {{ source('ingestion', 'order_items') }}
),
renamed as (
    select
        cast(id as bigint) as order_item_id,
        cast(order_id as bigint) as order_id,
        cast(product_id as bigint) as product_id,
        cast(quantity as bigint) as quantity,
        cast(unit_price as double) as unit_price,
        cast(coalesce(quantity, 0) * coalesce(unit_price, 0) as double) as line_amount
    from source
),
valid_orders as (
    select order_id
    from {{ ref('stg_orders') }}
),
validated as (
    select renamed.*
    from renamed
    inner join valid_orders
      on renamed.order_id = valid_orders.order_id
)
select * from validated
