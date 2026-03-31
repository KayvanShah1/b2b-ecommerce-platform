{{ config(materialized='incremental', unique_key='event_id') }}

with events as (
    select * from {{ ref('stg_webserver_logs') }}
    {% if is_incremental() %}
      where coalesce(_ingested_at, event_ts, cast('1900-01-01' as timestamp)) > (
        select coalesce(max(coalesce(_ingested_at, event_ts)), cast('1900-01-01' as timestamp))
        from {{ this }}
      )
    {% endif %}
)
select
    event_id,
    event_ts,
    event_date,
    remote_host,
    username,
    request_path,
    status_code,
    case
        when status_code between 200 and 299 then '2xx'
        when status_code between 300 and 399 then '3xx'
        when status_code between 400 and 499 then '4xx'
        when status_code between 500 and 599 then '5xx'
        else 'other'
    end as status_class,
    case when status_code >= 400 then true else false end as is_error,
    user_agent,
    _ingestion_run_id,
    _ingested_at,
    current_timestamp as loaded_at
from events
