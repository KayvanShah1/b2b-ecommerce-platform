# B2B E-commerce Pipeline (Core Ingestion)
[![Pipeline-Raw%20%2F%20Process%20%2F%20Load](https://img.shields.io/badge/Pipeline-Raw%20%2F%20Process%20%2F%20Load-0f766e?style=flat-square)](https://img.shields.io/)
[![State-Watermarks%20%2B%20Manifests](https://img.shields.io/badge/State-Watermarks%20%2B%20Manifests-1d4ed8?style=flat-square)](https://img.shields.io/)
[![Orchestrator-Agnostic](https://img.shields.io/badge/Orchestrator-Agnostic-black?style=flat-square)](https://img.shields.io/)

Core ingestion pipeline package (state, metadata, and execution primitives) with **no Dagster dependency**.
Dagster assets in [`b2b_ec_etl`](../../b2b_ec_etl/README.md) call into this package.

## High-Level Flow

```mermaid
flowchart LR
  subgraph SRC[Sources]
    PG[(Postgres tables)]
    CSV[(Marketing leads CSV)]
    LOG[(Webserver logs JSONL)]
  end

  subgraph RAW[Raw Capture]
    RAW_PG[Raw parquet: postgres]
    RAW_FILES[Raw parquet: files]
  end

  PROC[Process & Validate]
  PROCESSED[Processed parquet]
  LOAD[Staging Load]
  WH[(DuckDB / MotherDuck
  staging schema)]
  DBT[dbt models]

  STATE[(Postgres etl_metadata
  watermarks + manifests)]

  PG --> RAW_PG
  CSV --> RAW_FILES
  LOG --> RAW_FILES
  RAW_PG --> PROC
  RAW_FILES --> PROC
  PROC --> PROCESSED --> LOAD --> WH --> DBT

  RAW_PG -.-> STATE
  RAW_FILES -.-> STATE
  PROC -.-> STATE
  LOAD -.-> STATE
```

## What It Provides
1. **Raw capture**
   - Incremental extraction from Postgres (`full_snapshot`, `incremental_timestamp`, `incremental_id`)
   - File capture/cursoring for marketing leads (CSV) and webserver logs (JSONL)
   - Writes raw parquet to the configured storage backend
2. **Process**
   - Schema alignment to typed contracts (Pydantic models)
   - Validation + bad record counting
   - Deterministic de-dupe (file-level or dataset-level)
3. **Load**
   - Idempotent upsert into DuckDB/MotherDuck staging tables
   - Full-snapshot replacement support for selected targets
4. **State + metadata**
   - Watermarks, run manifests, checkpoints, and schema snapshots stored in Postgres (`etl_metadata` schema)
   - Optional cold-archive writes for audit/replay (via the snapshot/archive writer)

## Process Stage (High Level)

```mermaid
flowchart LR
  IN["Raw parquet inputs
  (raw_paths)"] --> READ[Read chunks]
  READ --> ALIGN["Align to contract schema
  (add missing columns)"]
  ALIGN --> PRE["Optional pre-processing
  (marketing_leads / webserver_logs)"]
  PRE --> VALIDATE["Validate + coerce types
  count/drop bad rows"]
  VALIDATE --> DEDUPE["Deterministic de-dupe
  (keys + sort column)"]
  DEDUPE --> OUT["Write processed parquet
  (processed_paths)"]
  OUT --> MANIFEST["Emit manifest + schema snapshot"]
```

## Metadata Watermark + State Resolver Decisions

```mermaid
flowchart TD
  START[Need manifests for process and load stages] --> INPUT{Were manifests provided by the caller?}
  INPUT -- Yes --> USE_INPUT[Use the provided manifests]
  INPUT -- No --> RESOLVE[Resolve manifests from state for each dataset]

  RESOLVE --> WM{Is there a stored watermark?}
  WM -- No --> NONE[No manifest available for this dataset]
  WM -- Yes --> RID{Does the watermark contain a run identifier?}
  RID -- No --> NONE
  RID -- Yes --> GET[Look up the manifest using run and dataset details]
  GET --> FOUND{Was a manifest found?}
  FOUND -- No --> NONE
  FOUND -- Yes --> VALID{Does it match the stage and have completed status?}
  VALID -- No --> NONE
  VALID -- Yes --> RESOLVED[Use this as the latest completed manifest]

  USE_INPUT --> COHERENCE{Do datasets resolve to mixed run identifiers?}
  RESOLVED --> COHERENCE
  NONE --> COHERENCE
  COHERENCE -- Yes and strict coherence is enabled --> ERROR[Stop with an error]
  COHERENCE -- Yes and strict coherence is disabled --> WARN[Log a warning and continue]
  COHERENCE -- No --> RUN_STAGE[Execute the stage]
  WARN --> RUN_STAGE

  RUN_STAGE --> COMMIT{Should watermark updates be committed?}
  COMMIT -- No --> RETURN_ONLY[Return manifests without writing watermark updates]
  COMMIT -- Yes --> LOOP[For each manifest:
  is a new watermark value available?]
  LOOP --> PUT[Write the watermark update]
  PUT --> COMMITTED[Watermarks committed]
```

## Package Layout
### `src/b2b_ec_pipeline/ingestion/`
- `postgres_raw.py`: extract source tables to raw parquet chunks
- `file_raw.py`: discover/capture new source files into raw parquet
- `process.py`: normalize/validate/dedupe raw datasets into processed parquet
- `staging.py`: load processed parquet into staging tables (DuckDB/MotherDuck)
- `models.py`: typed domain models + dataset specs used by raw/process/load

### `src/b2b_ec_pipeline/state/`
- `bootstrap.py`: creates/ensures the `etl_metadata` schema and tables
- `state_manager.py`: read/write watermarks + run manifests + run lifecycle helpers
- `snapshot_manager.py`: schema snapshots + checkpoints + archive hooks
- `archive.py`: archival writer for metadata artifacts

## How It's Used In This Repo
- **Orchestration:** `b2b_ec_etl` composes these primitives into Dagster assets and jobs. See [`ETL.md`](../../b2b_ec_etl/ETL.md).
- **Config + storage:** settings and storage backends come from `b2b-ec-utils`. See [`packages/b2b_ec_utils/README.md`](../b2b_ec_utils/README.md).

## Notes
- This package is intentionally not a standalone CLI; run it through Dagster (`b2b_ec_etl`) or the ingestion runners under `b2b_ec_etl/src/b2b_ec_etl/defs/ingestion/scripts/`.
