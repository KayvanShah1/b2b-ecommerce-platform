from dagster import AssetExecutionContext, MaterializeResult, asset


@asset(group_name="generation", required_resource_keys={"data_generation_resource"})
def source_db_generation(context: AssetExecutionContext) -> MaterializeResult:
    context.resources.data_generation_resource.run_source_generation()
    return MaterializeResult(metadata={"step": "postgres_seed_or_evolve"})


@asset(group_name="generation", required_resource_keys={"data_generation_resource"}, deps=[source_db_generation])
def marketing_leads_generation(context: AssetExecutionContext) -> MaterializeResult:
    context.resources.data_generation_resource.run_marketing_leads_generation()
    return MaterializeResult(metadata={"step": "marketing_leads_csv_generated"})


@asset(group_name="generation", required_resource_keys={"data_generation_resource"}, deps=[source_db_generation])
def webserver_logs_generation(context: AssetExecutionContext) -> MaterializeResult:
    context.resources.data_generation_resource.run_webserver_logs_generation()
    return MaterializeResult(metadata={"step": "webserver_logs_jsonl_generated"})


generation_assets = [source_db_generation, marketing_leads_generation, webserver_logs_generation]
