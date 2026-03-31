from dagster import AssetSelection, define_asset_job

# raw_capture_job = define_asset_job(
#     name="raw_capture_job",
#     selection=AssetSelection.assets("raw_capture_postgres", "raw_capture_files"),
#     description="Capture source datasets into raw storage with manifests/checkpoints/snapshots.",
# )

ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=AssetSelection.assets(
        "raw_capture_postgres",
        "raw_capture_files",
        "process_postgres",
        "process_files",
        "staging_incremental_load",
    ),
    description="Run raw capture, process, and incremental load to staging.",
)
