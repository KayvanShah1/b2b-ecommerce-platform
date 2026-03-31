with source as (
    select * from {{ source('ingestion', 'customers') }}
),
renamed as (
    select
        cast(id as bigint) as customer_id,
        company_cuit,
        document_number,
        username,
        first_name,
        last_name,
        trim(concat(coalesce(first_name, ''), ' ', coalesce(last_name, ''))) as customer_name,
        email,
        phone_number,
        cast(birth_date as date) as birth_date,
        cast(created_at as timestamp) as created_at
    from source
)
select * from renamed
