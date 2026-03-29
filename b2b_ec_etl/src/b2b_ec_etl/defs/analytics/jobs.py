from dagster import AssetSelection, define_asset_job

from b2b_ec_etl.defs.analytics.asset import dbt_models

job_build_dbt_models = define_asset_job(
    name="job_build_dbt_models",
    selection=AssetSelection.assets(dbt_models),
    description="Build analytics models with dbt.",
)

analytics_jobs = [job_build_dbt_models]
