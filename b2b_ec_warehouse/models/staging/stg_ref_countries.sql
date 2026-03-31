with source as (
    select * from {{ source('ingestion', 'ref_countries') }}
),
renamed as (
    select
        trim(code) as country_code,
        trim(name) as country_name
    from source
    order by country_code
)
select * from renamed
