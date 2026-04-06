from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

IngestionSource = Literal["postgres", "marketing_leads", "webserver_logs"]
IngestionStage = Literal["raw_capture", "process", "load"]
RunStatus = Literal["started", "completed", "failed"]
ExtractMode = Literal["full_snapshot", "incremental_timestamp", "incremental_id"]


class IngestionModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class Watermark(IngestionModel):
    source: IngestionSource | str
    dataset: str
    stage: IngestionStage
    mode: ExtractMode | Literal["file_incremental", "snapshot"] | None = None
    watermark_column: str | None = None
    value: str | int | float | datetime | None = None
    last_file: str | None = None
    last_line: int | None = None
    processed_files_count: int | None = None
    updated_at: datetime
    run_id: str


class IngestionCheckpoint(IngestionModel):
    source: IngestionSource | str
    dataset: str
    stage: IngestionStage
    run_id: str
    checkpoint_name: str
    checkpoint_value: str | int | float | datetime | None = None
    updated_at: datetime


class RunManifest(IngestionModel):
    source: IngestionSource | str
    dataset: str
    stage: IngestionStage
    run_id: str
    run_ts: datetime
    status: RunStatus
    record_count: int = 0
    processed_files: list[str] = Field(default_factory=list)
    raw_paths: list[str] = Field(default_factory=list)
    processed_paths: list[str] = Field(default_factory=list)
    bad_record_count: int = 0
    watermark_before: dict[str, Any] = Field(default_factory=dict)
    watermark_after: dict[str, Any] = Field(default_factory=dict)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    failed_at: datetime | None = None
    error_message: str | None = None


class SchemaColumnSnapshot(IngestionModel):
    name: str
    dtype: str
    nullable: bool = True


class DatasetSchemaSnapshot(IngestionModel):
    source: IngestionSource | str
    dataset: str
    stage: IngestionStage
    run_id: str
    captured_at: datetime
    columns: list[SchemaColumnSnapshot] = Field(default_factory=list)
    row_count: int | None = None
