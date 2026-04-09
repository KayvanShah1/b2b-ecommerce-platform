from __future__ import annotations

from datetime import datetime
from typing import Any

from b2b_ec_utils.logger import get_logger

ETL_METADATA_SCHEMA = "etl_metadata"

logger = get_logger("PipelineState")


def state_ref(schema: str, table: str, *parts: str) -> str:
    suffix = "/".join(str(part) for part in parts)
    return f"postgres://{schema}/{table}/{suffix}" if suffix else f"postgres://{schema}/{table}"


def to_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: to_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_json_safe(v) for v in value]
    return value
