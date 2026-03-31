{{ config(materialized='incremental', unique_key='order_id') }}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
      where coalesce(updated_at, order_ts, cast('1900-01-01' as timestamp)) >= (
        select coalesce(max(coalesce(source_updated_at, order_ts)), cast('1900-01-01' as timestamp))
        from {{ this }}
      )
    {% endif %}
),
order_item_stats as (
    select
        order_id,
        count(*) as order_item_count,
        sum(quantity) as total_quantity,
        sum(line_amount) as total_line_amount
    from {{ ref('stg_order_items') }}
    group by 1
)
select
    orders.order_id,
    orders.customer_id,
    orders.company_cuit,
    orders.order_status,
    orders.order_ts,
    orders.order_date,
    orders.order_total,
    coalesce(order_item_stats.order_item_count, 0) as order_item_count,
    coalesce(order_item_stats.total_quantity, 0) as total_quantity,
    coalesce(order_item_stats.total_line_amount, 0) as total_line_amount,
    orders.updated_at as source_updated_at,
    current_timestamp as loaded_at
from orders
left join order_item_stats
  on orders.order_id = order_item_stats.order_id
