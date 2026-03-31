with source as (
    select * from {{ source('ingestion', 'webserver_logs') }}
),
renamed as (
    select
        event_id,
        remote_host,
        username,
        cast(event_ts as timestamp) as event_ts,
        cast(event_ts as date) as event_date,
        user_agent,
        request_path,
        cast(status_code as integer) as status_code,
        _source_file,
        cast(_source_line as bigint) as _source_line,
        _ingestion_run_id,
        cast(_ingested_at as timestamp) as _ingested_at
    from source
)
select * from renamed
