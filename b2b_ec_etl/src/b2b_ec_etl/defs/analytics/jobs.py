from dagster import AssetSelection, define_asset_job

from b2b_ec_etl.defs.analytics.asset import dbt_models

data_transformations_job = define_asset_job(
    name="data_transformations_job",
    selection=AssetSelection.assets(dbt_models),
    description="Transform data and build analytics models with dbt for downstream consumption.",
)

analytics_jobs = [data_transformations_job]
