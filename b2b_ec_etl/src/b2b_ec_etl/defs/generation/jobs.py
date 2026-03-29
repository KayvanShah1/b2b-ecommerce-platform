from dagster import AssetSelection, define_asset_job


generation_simulation_job = define_asset_job(
    name="generation_simulation_job",
    selection=AssetSelection.groups("generation"),
)
