-- Report Question:
-- All sales of B2B platform displayed monthly for the last year.
--
-- Assumption:
-- "Sales" are recognized from COMPLETED orders, with RETURNED orders shown separately.

with months as (
    select cast(series.generate_series as date) as month_start
    from generate_series(
        date_trunc('month', current_date - interval '11 month'),
        date_trunc('month', current_date),
        interval '1 month'
    ) as series
),
monthly_order_metrics as (
    select
        cast(date_trunc('month', order_date) as date) as month_start,
        sum(case when order_status = 'COMPLETED' then coalesce(order_total, 0) else 0 end) as gross_sales_amount,
        sum(case when order_status = 'RETURNED' then coalesce(order_total, 0) else 0 end) as returned_amount,
        sum(case when order_status = 'COMPLETED' then 1 else 0 end) as completed_orders,
        sum(case when order_status = 'RETURNED' then 1 else 0 end) as returned_orders,
        sum(case when order_status = 'CANCELLED' then 1 else 0 end) as cancelled_orders
    from {{ ref('fct_orders') }}
    where order_date >= date_trunc('month', current_date - interval '11 month')
      and order_date < date_trunc('month', current_date + interval '1 month')
    group by 1
)
select
    months.month_start,
    round(coalesce(metrics.gross_sales_amount, 0), 2) as gross_sales_amount,
    round(coalesce(metrics.returned_amount, 0), 2) as returned_amount,
    round(coalesce(metrics.gross_sales_amount, 0) - coalesce(metrics.returned_amount, 0), 2) as net_sales_amount,
    coalesce(metrics.completed_orders, 0) as completed_orders,
    coalesce(metrics.returned_orders, 0) as returned_orders,
    coalesce(metrics.cancelled_orders, 0) as cancelled_orders
from months
left join monthly_order_metrics as metrics
  on months.month_start = metrics.month_start
where gross_sales_amount > 0
order by months.month_start
