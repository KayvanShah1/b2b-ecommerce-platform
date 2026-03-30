from dagster import AssetSelection, define_asset_job

from b2b_ec_etl.defs.analytics.asset import dbt_models

build_analytics_models = define_asset_job(
    name="build_analytics_models",
    selection=AssetSelection.assets(dbt_models),
    description="Build analytics models with dbt.",
)

analytics_jobs = [build_analytics_models]
