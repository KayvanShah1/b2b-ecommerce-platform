{{ config(materialized='incremental', unique_key=['company_cuit', 'product_id']) }}

with source as (
    select * from {{ ref('stg_company_catalogs') }}
    {% if is_incremental() %}
      where coalesce(updated_at, created_at, cast('1900-01-01' as timestamp)) >= (
        select coalesce(max(coalesce(updated_at, created_at)), cast('1900-01-01' as timestamp))
        from {{ this }}
      )
    {% endif %}
),
latest as (
    select
        *,
        row_number() over (
            partition by company_cuit, product_id
            order by coalesce(updated_at, created_at) desc, created_at desc
        ) as rn
    from source
)
select
    company_cuit,
    product_id,
    sale_price as current_sale_price,
    created_at,
    updated_at,
    current_timestamp as loaded_at
from latest
where rn = 1
