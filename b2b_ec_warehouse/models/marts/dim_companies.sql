with companies as (
    select * from {{ ref('stg_companies') }}
),
countries as (
    select * from {{ ref('dim_countries') }}
)
select
    companies.company_cuit,
    companies.company_name,
    companies.company_type,
    companies.country_code,
    countries.country_name,
    companies.created_at,
    companies.updated_at
from companies
left join countries
  on companies.country_code = countries.country_code
