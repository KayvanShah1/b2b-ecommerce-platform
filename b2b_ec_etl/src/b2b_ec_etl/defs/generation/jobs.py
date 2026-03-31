from dagster import AssetSelection, define_asset_job

data_generation_simulation_job = define_asset_job(
    name="data_generation_simulation_job",
    selection=AssetSelection.groups("data_source_generation"),
    description="Generate source data and simulate evolution for commerce data, marketing leads, and webserver logs.",
)
