from dagster import AssetSelection, define_asset_job

data_ingestion_job = define_asset_job(
    name="data_ingestion_job",
    selection=AssetSelection.assets(
        "raw_capture_postgres",
        "raw_capture_files",
        "process_postgres",
        "process_files",
        "staging_incremental_load",
    ),
    description="Run raw capture, process, and incremental load to ingestion schema into MotherDuck.",
)
