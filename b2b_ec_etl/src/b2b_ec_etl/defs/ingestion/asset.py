from datetime import datetime, timezone

from dagster import AssetExecutionContext, MaterializeResult, asset


@asset(group_name="ingestion", required_resource_keys={"ingestion_resource"}, kinds=["python", "postgres"])
def raw_capture_postgres(context: AssetExecutionContext) -> list[dict]:
    run_ts = datetime.now(timezone.utc)
    manifests = context.resources.ingestion_resource.raw_capture_postgres(run_id=context.run_id, run_ts=run_ts)
    total_rows = sum(m["record_count"] for m in manifests)
    context.add_output_metadata({"datasets": len(manifests), "records": total_rows})
    return manifests


@asset(group_name="ingestion", required_resource_keys={"ingestion_resource"}, kinds=["python", "s3"])
def raw_capture_files(context: AssetExecutionContext) -> dict:
    run_ts = datetime.now(timezone.utc)
    manifests = context.resources.ingestion_resource.raw_capture_files(run_id=context.run_id, run_ts=run_ts)
    total_rows = manifests["marketing_leads"]["record_count"] + manifests["webserver_logs"]["record_count"]
    context.add_output_metadata({"datasets": 2, "records": total_rows})
    return manifests


@asset(
    group_name="ingestion",
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
    total_rows = sum(m["record_count"] for m in manifests)
    total_bad_rows = sum(m.get("bad_record_count", 0) for m in manifests)
    context.add_output_metadata(
        {"datasets": len(manifests), "processed_records": total_rows, "bad_records": total_bad_rows}
    )
    return manifests


@asset(
    group_name="ingestion",
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
    total_rows = sum(m["record_count"] for m in manifests.values())
    total_bad_rows = sum(m.get("bad_record_count", 0) for m in manifests.values())
    context.add_output_metadata(
        {"datasets": len(manifests), "processed_records": total_rows, "bad_records": total_bad_rows}
    )
    return manifests


@asset(
    group_name="ingestion",
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
