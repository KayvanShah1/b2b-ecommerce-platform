# B2B E-commerce Utilities
[![Config-Pydantic%20Settings](https://img.shields.io/badge/Config-Pydantic%20Settings-0f766e?style=flat-square)](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
[![Storage-fsspec](https://img.shields.io/badge/Storage-fsspec-1d4ed8?style=flat-square)](https://filesystem-spec.readthedocs.io/)
[![Logging-Rich%20%2B%20Rotating%20Files](https://img.shields.io/badge/Logging-Rich%20%2B%20Rotating%20Files-f97316?style=flat-square)](https://rich.readthedocs.io/)

Shared utilities package used by source generation and ETL orchestration.

## What This Package Provides
1. Centralized application settings loaded from `.env` via Pydantic.
2. Unified storage abstraction for `local`, `s3`, `minio`, and `gcs` via `fsspec`.
3. Consistent structured logging (console + rotating file).
4. Timing decorator for runtime metrics.

## Modules
1. `settings.py`
   - `settings` singleton with grouped config sections:
     - `postgres` (`POSTGRES_*`)
     - `storage` (`STORAGE_*`, `STORAGE_DATASET_*`)
     - `motherduck` (`MOTHERDUCK_*`)
     - `dbt` (`DBT_*`)
     - `ingestion` (`INGESTION_*`, including `INGESTION_LOAD_SCHEMA`)
   - Auto-detects project root from `.env`.
   - Ensures runtime directories exist under `var/`.
2. `storage.py`
   - `storage` singleton with URI/path helpers for:
     - marketing leads
     - webserver logs
     - ingestion raw/processed data
     - ingestion metadata (watermarks, runs, lineage)
   - Supports idempotent bucket creation and local directory auto-create on writes.
3. `logger.py`
   - `get_logger(name)` with:
     - Rich console output
     - rotating file logs in `var/logs/`
4. `timer.py`
   - `@timed_run` decorator to log wall and CPU execution time.

## Quick Usage

```python
from b2b_ec_utils import get_logger, settings, timed_run
from b2b_ec_utils.storage import storage

logger = get_logger("Example")
logger.info("Storage mode: %s", settings.storage.location)

raw_orders_path = storage.get_raw_dataset_path("postgres", "orders", "part-000.parquet")
logger.info("Raw orders path: %s", raw_orders_path)

@timed_run
def run():
    logger.info("Running task")
```

## Run Commands (uv)

```bash
# from repository root
uv sync --all-packages

# start local MinIO (optional, recommended for object-storage mode)
docker compose up -d minio

# print resolved settings
uv run python packages/b2b_ec_utils/src/b2b_ec_utils/settings.py

# quick storage initialization/bucket check
uv run python packages/b2b_ec_utils/src/b2b_ec_utils/storage.py
```

## MinIO Configuration
Use these `.env` variables for local MinIO:
1. `STORAGE_LOCATION=minio`
2. `STORAGE_MINIO_ENDPOINT_URL=http://localhost:9000`
3. `STORAGE_MINIO_ROOT_USER=<your-user>`
4. `STORAGE_MINIO_ROOT_PASSWORD=<your-password>`
5. `STORAGE_PREFIX=b2b-ec` (optional, defaults to `b2b-ec`)

Default bucket names generated from prefix:
1. `<prefix>-webserver-logs`
2. `<prefix>-marketing-leads`
3. `<prefix>-raw-data`
4. `<prefix>-processed-data`
5. `<prefix>-metadata`

## Notes
1. Default ingestion load schema is `ingestion` (`INGESTION_LOAD_SCHEMA` can override).
2. Bucket names are prefixed by `STORAGE_PREFIX` (default `b2b-ec`).
