# B2B E-commerce Data Platform (Dagster ETL + dbt)
[![Orchestration-Dagster](https://img.shields.io/badge/Orchestration-Dagster-4f46e5?style=flat-square)](https://dagster.io/)
[![Warehouse-MotherDuck%20%2F%20DuckDB](https://img.shields.io/badge/Warehouse-MotherDuck%20%2F%20DuckDB-0f766e?style=flat-square)](https://motherduck.com/)
[![Transformations-dbt](https://img.shields.io/badge/Transformations-dbt-f97316?style=flat-square)](https://www.getdbt.com/)

Dagster project for orchestrating the B2B ecommerce pipeline:
1. Ingestion flow: raw capture -> processing -> incremental load into the ingestion schema.
2. Analytics flow: run dbt models for staging and marts.

## Jobs
1. `data_ingestion_job`
   Runs:
   - `raw_capture_postgres`
   - `raw_capture_files`
   - `process_postgres`
   - `process_files`
   - `staging_incremental_load`
2. `data_transformations_job`
   Runs dbt assets from `b2b_ec_warehouse`.

## Setup (uv)
From repository root:

```bash
uv sync --all-packages
```

## Run (uv)
Start Dagster UI:

```bash
cd b2b_ec_etl
uv run dg dev
```

Dagster will be available at `http://localhost:3000`.

## Optional Direct dbt Run
If you want to run dbt directly (outside Dagster):

```bash
uv run dbt build --project-dir b2b_ec_warehouse --profiles-dir .dbt
```

## PostgreSQL Source Setup (Aiven Cloud)
The ingestion raw-capture stage reads source tables from Postgres.

Required `.env` keys:
1. `POSTGRES_HOST`
2. `POSTGRES_PORT`
3. `POSTGRES_USER`
4. `POSTGRES_PASSWORD`
5. `POSTGRES_DATABASE`
6. `POSTGRES_SSLMODE=require` (recommended for Aiven)
7. `POSTGRES_SSLROOTCERT=<path-to-ca.pem>` (optional)

## MinIO Storage (Recommended Local Mode)
ETL stages use object storage for raw, processed, and metadata artifacts.

```bash
# from repository root
docker compose up -d minio
uv run python packages/b2b_ec_utils/src/b2b_ec_utils/storage.py
```

Key `.env` variables:
1. `STORAGE_LOCATION=minio`
2. `STORAGE_MINIO_ENDPOINT_URL=http://localhost:9000`
3. `STORAGE_MINIO_ROOT_USER=<your-user>`
4. `STORAGE_MINIO_ROOT_PASSWORD=<your-password>`

## Notes
1. Keep `.env` configured for Postgres, storage, and MotherDuck before running jobs.
2. Ingestion target schema is controlled by `INGESTION_LOAD_SCHEMA`.
