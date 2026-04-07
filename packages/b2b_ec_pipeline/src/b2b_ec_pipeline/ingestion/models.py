from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import Field

from b2b_ec_pipeline.state.models import ExtractMode, IngestionModel, IngestionSource, RunManifest


######################################
# Commerce Domain Models
######################################
class RefCountryRow(IngestionModel):
    code: str
    name: str


class CompanyRow(IngestionModel):
    cuit: str
    name: str
    type: Literal["Supplier", "Client"]
    country_code: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ProductRow(IngestionModel):
    id: int
    name: str
    supplier_cuit: str | None = None
    base_price: Decimal | float | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CompanyCatalogRow(IngestionModel):
    company_cuit: str
    product_id: int
    sale_price: Decimal | float | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CustomerRow(IngestionModel):
    id: int
    company_cuit: str | None = None
    document_number: str
    username: str
    first_name: str
    last_name: str
    email: str
    phone_number: str | None = None
    birth_date: date
    created_at: datetime | None = None


class OrderRow(IngestionModel):
    id: int
    company_cuit: str | None = None
    customer_id: int | None = None
    status: Literal["COMPLETED", "CANCELLED", "RETURNED"]
    order_date: datetime | None = None
    total_amount: Decimal | float | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class OrderItemRow(IngestionModel):
    id: int
    order_id: int
    product_id: int | None = None
    quantity: int | None = None
    unit_price: Decimal | float | None = None


POSTGRES_TABLE_SCHEMAS: dict[str, type[IngestionModel]] = {
    "ref_countries": RefCountryRow,
    "companies": CompanyRow,
    "products": ProductRow,
    "company_catalogs": CompanyCatalogRow,
    "customers": CustomerRow,
    "orders": OrderRow,
    "order_items": OrderItemRow,
}


#######################################
# Source File Domain Models
#######################################
class MarketingLeadRow(IngestionModel):
    lead_id: str
    created_at: datetime
    company_name: str
    is_prospect: bool
    industry: str
    contact_name: str
    contact_email: str
    contact_phone: str | None = None
    lead_source: str
    estimated_annual_revenue: float
    country_code: str
    status: str
    status_updated_at: datetime | None = None
    last_activity_at: datetime | None = None
    _source_file: str | None = None
    _ingestion_run_id: str | None = None
    _ingested_at: datetime | None = None


class WebServerLogRawRow(IngestionModel):
    event_id: str
    remote_host: str
    ident: str | None = None
    username: str | None = None
    timestamp: datetime
    request_method: str | None = None
    request_path: str | None = None
    http_version: str | None = None
    status_code: int | None = None
    response_size_bytes: int | None = None
    referer: str | None = None
    user_agent: str | None = None


class WebServerLogProcessedRow(IngestionModel):
    event_id: str
    remote_host: str | None = None
    username: str | None = None
    event_ts: datetime
    user_agent: str | None = None
    request_path: str | None = None
    status_code: int | None = None
    _source_file: str
    _source_line: int
    _ingestion_run_id: str
    _ingested_at: datetime


FILE_SOURCE_SCHEMAS: dict[str, type[IngestionModel]] = {
    "marketing_leads": MarketingLeadRow,
    "webserver_logs_raw": WebServerLogRawRow,
    "webserver_logs_processed": WebServerLogProcessedRow,
}


############################################################
# Process/Load Config Models
############################################################
class ProcessDatasetSpec(IngestionModel):
    source: IngestionSource
    dataset: str
    model_key: str
    dedupe_keys: tuple[str, ...] = ()
    dedupe_sort_column: str | None = None
    dedupe_scope: Literal["file", "dataset"] = "file"
    preprocess: Literal["marketing_leads", "webserver_logs"] | None = None


class LoadTargetSpec(IngestionModel):
    table: str
    primary_key: tuple[str, ...] = ()
    full_snapshot: bool = False


class LoadDatasetSpec(IngestionModel):
    source: IngestionSource
    dataset: str
    mode: ExtractMode | Literal["file_incremental", "snapshot"] = "file_incremental"
    targets: list[LoadTargetSpec]


class RawFileCaptureSpec(IngestionModel):
    source: IngestionSource
    dataset: str
    pattern_keys: tuple[Literal["marketing_csv", "web_logs_seed_jsonl", "web_logs_daily_jsonl"], ...]
    loader: Literal["marketing_csv", "web_logs_jsonl"]


class ManifestBundle(IngestionModel):
    postgres: list[RunManifest] = Field(default_factory=list)
    files: dict[str, RunManifest] = Field(default_factory=dict)


class PostgresLoadResult(IngestionModel):
    manifests: list[RunManifest] = Field(default_factory=list)
    loaded_rows: int = 0
    loaded_tables: list[str] = Field(default_factory=list)


class FileLoadResult(IngestionModel):
    manifests: dict[str, RunManifest] = Field(default_factory=dict)
    loaded_rows: int = 0
    loaded_tables: list[str] = Field(default_factory=list)


class LoadBundle(IngestionModel):
    postgres: PostgresLoadResult
    files: FileLoadResult


################################################
# Postgres Table Config Models & Consolidation
###############################################
@dataclass(frozen=True)
class PostgresTableConfig:
    name: str
    primary_key: tuple[str, ...]
    mode: ExtractMode
    watermark_column: str | None = None


POSTGRES_TABLE_CONFIGS: tuple[PostgresTableConfig, ...] = (
    PostgresTableConfig(name="ref_countries", primary_key=("code",), mode="full_snapshot"),
    PostgresTableConfig(
        name="companies", primary_key=("cuit",), mode="incremental_timestamp", watermark_column="updated_at"
    ),
    PostgresTableConfig(
        name="products", primary_key=("id",), mode="incremental_timestamp", watermark_column="updated_at"
    ),
    PostgresTableConfig(
        name="company_catalogs",
        primary_key=("company_cuit", "product_id"),
        mode="incremental_timestamp",
        watermark_column="updated_at",
    ),
    PostgresTableConfig(
        name="customers", primary_key=("id",), mode="incremental_timestamp", watermark_column="created_at"
    ),
    PostgresTableConfig(
        name="orders", primary_key=("id",), mode="incremental_timestamp", watermark_column="updated_at"
    ),
    PostgresTableConfig(name="order_items", primary_key=("id",), mode="incremental_id", watermark_column="id"),
)


FILE_PROCESS_SPECS: dict[str, ProcessDatasetSpec] = {
    "marketing_leads": ProcessDatasetSpec(
        source="marketing_leads",
        dataset="marketing_leads",
        model_key="marketing_leads",
        dedupe_keys=("lead_id",),
        dedupe_sort_column="status_updated_at",
        dedupe_scope="dataset",
        preprocess="marketing_leads",
    ),
    "webserver_logs": ProcessDatasetSpec(
        source="webserver_logs",
        dataset="webserver_logs",
        model_key="webserver_logs_processed",
        dedupe_keys=("event_id",),
        dedupe_sort_column="event_ts",
        preprocess="webserver_logs",
    ),
}


FILE_LOAD_SPECS: dict[str, LoadDatasetSpec] = {
    "marketing_leads": LoadDatasetSpec(
        source="marketing_leads",
        dataset="marketing_leads",
        mode="file_incremental",
        targets=[
            LoadTargetSpec(table="marketing_leads_current", primary_key=("lead_id",), full_snapshot=False),
            LoadTargetSpec(
                table="marketing_leads_history",
                primary_key=("lead_id", "_ingestion_run_id"),
                full_snapshot=False,
            ),
        ],
    ),
    "webserver_logs": LoadDatasetSpec(
        source="webserver_logs",
        dataset="webserver_logs",
        mode="file_incremental",
        targets=[LoadTargetSpec(table="webserver_logs", primary_key=("event_id",), full_snapshot=False)],
    ),
}


FILE_RAW_CAPTURE_SPECS: dict[str, RawFileCaptureSpec] = {
    "marketing_leads": RawFileCaptureSpec(
        source="marketing_leads",
        dataset="marketing_leads",
        pattern_keys=("marketing_csv",),
        loader="marketing_csv",
    ),
    "webserver_logs": RawFileCaptureSpec(
        source="webserver_logs",
        dataset="webserver_logs",
        pattern_keys=("web_logs_seed_jsonl", "web_logs_daily_jsonl"),
        loader="web_logs_jsonl",
    ),
}
