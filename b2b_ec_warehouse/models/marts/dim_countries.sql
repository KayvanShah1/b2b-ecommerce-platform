with countries as (
    select * from {{ ref('stg_ref_countries') }}
)
select
    country_code,
    country_name
from countries
