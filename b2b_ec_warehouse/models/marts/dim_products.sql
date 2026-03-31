with products as (
    select * from {{ ref('stg_products') }}
),
suppliers as (
    select * from {{ ref('dim_companies') }}
),
catalog_stats as (
    select
        product_id,
        count(distinct company_cuit) as listed_by_company_count,
        min(sale_price) as min_catalog_sale_price,
        max(sale_price) as max_catalog_sale_price,
        avg(sale_price) as avg_catalog_sale_price
    from {{ ref('stg_company_catalogs') }}
    group by 1
)
select
    products.product_id,
    products.product_name,
    products.supplier_cuit,
    suppliers.company_name as supplier_name,
    round(products.base_price, 2) as base_price,
    round(products.product_price, 2) as product_price,
    catalog_stats.listed_by_company_count,
    round(catalog_stats.min_catalog_sale_price, 2) as min_catalog_sale_price,
    round(catalog_stats.max_catalog_sale_price, 2) as max_catalog_sale_price,
    round(catalog_stats.avg_catalog_sale_price, 2) as avg_catalog_sale_price,
    products.created_at,
    products.updated_at
from products
left join suppliers
  on products.supplier_cuit = suppliers.company_cuit
left join catalog_stats
  on products.product_id = catalog_stats.product_id
