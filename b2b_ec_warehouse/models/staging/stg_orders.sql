with source as (
    select * from {{ source('ingestion', 'orders') }}
),
renamed as (
    select
        cast(id as bigint) as order_id,
        company_cuit,
        cast(customer_id as bigint) as customer_id,
        status as order_status,
        cast(order_date as timestamp) as order_ts,
        cast(order_date as date) as order_date,
        cast(total_amount as double) as order_total,
        cast(total_amount as double) as subtotal,
        cast(0 as double) as tax_paid,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
)
select * from renamed
