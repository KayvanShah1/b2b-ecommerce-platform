with customers as (
    select * from {{ ref('stg_customers') }}
),
companies as (
    select * from {{ ref('dim_companies') }}
)
select
    customers.customer_id,
    customers.company_cuit,
    companies.company_name as customer_company_name,
    customers.document_number,
    customers.username,
    customers.first_name,
    customers.last_name,
    customers.customer_name,
    customers.email,
    customers.phone_number,
    customers.birth_date,
    customers.created_at
from customers
left join companies
  on customers.company_cuit = companies.company_cuit
