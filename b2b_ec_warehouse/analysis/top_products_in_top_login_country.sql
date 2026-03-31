-- Report Question:
-- What are the most popular products in the country from which most users log in?
--
-- Note:
-- We infer login country using username -> customer -> company country mapping.
-- (IP geolocation is not modeled yet in the warehouse.)

with login_events_by_country as (
    select
        coalesce(companies.country_name, 'Unknown') as country_name,
        count(*) as login_event_count,
        count(distinct logs.username) as unique_logged_in_users
    from {{ ref('fct_webserver_logs') }} as logs
    join {{ ref('dim_customers') }} as customers
      on logs.username = customers.username
    left join {{ ref('dim_companies') }} as companies
      on customers.company_cuit = companies.company_cuit
    where logs.username is not null
      and logs.username <> '-'
    group by 1
),
top_login_country as (
    select country_name, login_event_count, unique_logged_in_users
    from login_events_by_country
    qualify row_number() over (order by login_event_count desc, unique_logged_in_users desc, country_name) = 1
),
product_popularity as (
    select
        top_country.country_name,
        order_items.product_id,
        coalesce(order_items.product_name, products.product_name) as product_name,
        sum(coalesce(order_items.quantity, 0)) as total_units_sold,
        round(sum(coalesce(order_items.line_amount, 0)), 2) as total_sales_amount,
        count(distinct order_items.order_id) as order_count
    from {{ ref('fct_order_items') }} as order_items
    join {{ ref('fct_orders') }} as orders
      on order_items.order_id = orders.order_id
    join {{ ref('dim_companies') }} as companies
      on orders.company_cuit = companies.company_cuit
    join top_login_country as top_country
      on companies.country_name = top_country.country_name
    left join {{ ref('dim_products') }} as products
      on order_items.product_id = products.product_id
    group by 1, 2, 3
)
select
    country_name,
    product_id,
    product_name,
    total_units_sold,
    total_sales_amount,
    order_count
from product_popularity
order by total_units_sold desc, total_sales_amount desc, product_name
limit 10
