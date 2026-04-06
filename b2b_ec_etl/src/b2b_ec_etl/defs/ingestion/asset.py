from datetime import datetime, timezone

from dagster import AssetExecutionContext, MaterializeResult, asset


def _manifest_list(payload: list[dict] | dict[str, dict]) -> list[dict]:
    return payload if isinstance(payload, list) else list(payload.values())


def _manifest_metrics(payload: list[dict] | dict[str, dict]) -> dict[str, int]:
    manifests = _manifest_list(payload)
    return {
        "datasets": len(manifests),
        "records": sum(int(m.get("record_count", 0)) for m in manifests),
        "bad_records": sum(int(m.get("bad_record_count", 0)) for m in manifests),
    }


@asset(group_name="raw_data_capture", required_resource_keys={"ingestion_resource"}, kinds=["python", "postgres"])
def raw_capture_postgres(context: AssetExecutionContext) -> list[dict]:
    run_ts = datetime.now(timezone.utc)
    manifests = context.resources.ingestion_resource.raw_capture_postgres(run_id=context.run_id, run_ts=run_ts)
    metrics = _manifest_metrics(manifests)
    context.add_output_metadata({"datasets": metrics["datasets"], "records": metrics["records"]})
    return manifests


@asset(group_name="raw_data_capture", required_resource_keys={"ingestion_resource"}, kinds=["python", "s3"])
def raw_capture_files(context: AssetExecutionContext) -> dict:
    run_ts = datetime.now(timezone.utc)
    manifests = context.resources.ingestion_resource.raw_capture_files(run_id=context.run_id, run_ts=run_ts)
    metrics = _manifest_metrics(manifests)
    context.add_output_metadata(
        {
            "datasets": metrics["datasets"],
            "records": metrics["records"],
            "bad_records": metrics["bad_records"],
        }
    )
    return manifests


@asset(
    group_name="light_processing",
    required_resource_keys={"ingestion_resource"},
    deps=[raw_capture_postgres],
    kinds=["polars", "s3"],
)
def process_postgres(
    context: AssetExecutionContext,
    raw_capture_postgres: list[dict],
) -> list[dict]:
    run_ts = datetime.now(timezone.utc)
    manifests = context.resources.ingestion_resource.process_postgres_manifests(
        run_id=context.run_id,
        run_ts=run_ts,
        raw_manifests=raw_capture_postgres,
    )
    metrics = _manifest_metrics(manifests)
    context.add_output_metadata(
        {
            "datasets": metrics["datasets"],
            "processed_records": metrics["records"],
            "bad_records": metrics["bad_records"],
        }
    )
    return manifests


@asset(
    group_name="light_processing",
    required_resource_keys={"ingestion_resource"},
    deps=[raw_capture_files],
    kinds=["polars", "s3"],
)
def process_files(
    context: AssetExecutionContext,
    raw_capture_files: dict,
) -> dict:
    run_ts = datetime.now(timezone.utc)
    manifests = context.resources.ingestion_resource.process_file_manifests(
        run_id=context.run_id,
        run_ts=run_ts,
        raw_file_manifests=raw_capture_files,
    )
    metrics = _manifest_metrics(manifests)
    context.add_output_metadata(
        {
            "datasets": metrics["datasets"],
            "processed_records": metrics["records"],
            "bad_records": metrics["bad_records"],
        }
    )
    return manifests


@asset(
    group_name="incremental_load",
    required_resource_keys={"ingestion_resource", "duckdb"},
    deps=[process_postgres, process_files],
    kinds=["python", "duckdb"],
)
def staging_incremental_load(
    context: AssetExecutionContext,
    process_postgres: list[dict],
    process_files: dict,
) -> MaterializeResult:
    run_ts = datetime.now(timezone.utc)
    with context.resources.duckdb.get_connection() as conn:
        result = context.resources.ingestion_resource.load_all_to_staging(
            conn=conn,
            run_id=context.run_id,
            run_ts=run_ts,
            postgres_manifests=process_postgres,
            file_manifests=process_files,
        )
    postgres_loaded = result["postgres"]["loaded_rows"]
    files_loaded = result["files"]["loaded_rows"]
    return MaterializeResult(
        metadata={
            "postgres_loaded_rows": postgres_loaded,
            "file_loaded_rows": files_loaded,
            "total_loaded_rows": postgres_loaded + files_loaded,
        }
    )


ingestion_assets = [raw_capture_postgres, raw_capture_files, process_postgres, process_files, staging_incremental_load]
