{{ config(materialized='incremental', unique_key='order_item_id') }}

with order_items as (
    select * from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
      where order_item_id > (select coalesce(max(order_item_id), 0) from {{ this }})
    {% endif %}
),
orders as (
    select order_id, customer_id, company_cuit, order_ts, order_date
    from {{ ref('stg_orders') }}
),
products as (
    select product_id, product_name, supplier_cuit, base_price
    from {{ ref('stg_products') }}
)
select
    order_items.order_item_id,
    order_items.order_id,
    orders.order_ts,
    orders.order_date,
    orders.customer_id,
    orders.company_cuit,
    order_items.product_id,
    products.product_name,
    products.supplier_cuit,
    order_items.quantity,
    order_items.unit_price,
    order_items.line_amount,
    cast(coalesce(order_items.quantity, 0) * coalesce(products.base_price, 0) as double) as estimated_base_cost,
    cast(
      coalesce(order_items.line_amount, 0) - (coalesce(order_items.quantity, 0) * coalesce(products.base_price, 0))
      as double
    ) as estimated_margin,
    current_timestamp as loaded_at
from order_items
left join orders
  on order_items.order_id = orders.order_id
left join products
  on order_items.product_id = products.product_id
