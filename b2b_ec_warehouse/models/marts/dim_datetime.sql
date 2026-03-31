with candidate_dates as (
    select cast(order_ts as date) as date_day
    from {{ ref('stg_orders') }}
    where order_ts is not null

    union all

    select cast(event_ts as date) as date_day
    from {{ ref('stg_webserver_logs') }}
    where event_ts is not null

    union all

    select cast(created_at as date) as date_day
    from {{ ref('stg_marketing_leads_history') }}
    where created_at is not null

    union all

    select cast(status_updated_at as date) as date_day
    from {{ ref('stg_marketing_leads_history') }}
    where status_updated_at is not null

    union all

    select cast(last_activity_at as date) as date_day
    from {{ ref('stg_marketing_leads_history') }}
    where last_activity_at is not null
),
bounds as (
    select
        coalesce(min(date_day), current_date - interval '365 day') as min_date,
        coalesce(max(date_day), current_date + interval '365 day') as max_date
    from candidate_dates
),
calendar as (
    select cast(series.generate_series as date) as date_day
    from bounds,
        generate_series(bounds.min_date, bounds.max_date, interval 1 day) as series
)
select
    cast(strftime(date_day, '%Y%m%d') as bigint) as date_key,
    date_day,
    cast(strftime(date_day, '%Y') as integer) as year_num,
    cast(quarter(date_day) as integer) as quarter_num,
    cast(strftime(date_day, '%m') as integer) as month_num,
    strftime(date_day, '%B') as month_name,
    cast(strftime(date_day, '%d') as integer) as day_of_month,
    cast(strftime(date_day, '%j') as integer) as day_of_year,
    cast(strftime(date_day, '%u') as integer) as iso_day_of_week,
    strftime(date_day, '%A') as day_name,
    cast(strftime(date_day, '%V') as integer) as iso_week_of_year,
    case when cast(strftime(date_day, '%u') as integer) in (6, 7) then true else false end as is_weekend,
    cast(date_trunc('month', date_day) as date) as month_start_date,
    cast(date_trunc('year', date_day) as date) as year_start_date,
    cast(date_day as timestamp) as day_start_ts,
    cast(date_day + interval 1 day - interval 1 second as timestamp) as day_end_ts
from calendar
