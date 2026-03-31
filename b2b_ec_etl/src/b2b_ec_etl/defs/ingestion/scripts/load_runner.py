from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import duckdb
from b2b_ec_utils.logger import get_logger

from b2b_ec_etl.defs.ingestion.resources import IngestionResource, resolve_duckdb_database_uri

logger = get_logger("LoadRunner")


def _safe_db_uri(uri: str) -> str:
    for token_key in ("motherduck_token=", "token="):
        if token_key in uri:
            prefix = uri.split(token_key, 1)[0]
            return f"{prefix}{token_key}***"
    return uri


def run_load_only(run_id: str | None = None) -> dict[str, Any]:
    run_ts = datetime.now(timezone.utc)
    effective_run_id = run_id or f"manual-load-{run_ts.strftime('%Y%m%dT%H%M%S')}"
    logger.info(f"LOAD ONLY START: run_id={effective_run_id}")

    resource = IngestionResource()
    postgres_manifests, file_manifests = resource.resolve_process_manifests()
    logger.info(
        f"LOAD ONLY MANIFESTS: run_id={effective_run_id} "
        f"postgres={len(postgres_manifests)} files={len(file_manifests)}"
    )

    if not postgres_manifests and not file_manifests:
        logger.warning(f"LOAD ONLY SKIP: run_id={effective_run_id} no process manifests found")
        return {
            "run_id": effective_run_id,
            "load": {
                "postgres": {"manifests": [], "loaded_rows": 0, "loaded_tables": []},
                "files": {"manifests": {}, "loaded_rows": 0, "loaded_tables": []},
            },
        }

    # Keep runner simple: always use configured warehouse target (MotherDuck when token is configured).
    database_uri = resolve_duckdb_database_uri(prefer_local=False)
    logger.info(f"LOAD ONLY CONNECT: run_id={effective_run_id} database={_safe_db_uri(database_uri)}")

    with duckdb.connect(database=database_uri) as conn:
        logger.info(f"LOAD ONLY CONNECTED: run_id={effective_run_id}")
        conn.execute("SELECT 1")
        logger.info(f"LOAD ONLY PING OK: run_id={effective_run_id}")
        result = resource.load_all_to_staging(
            conn=conn,
            run_id=effective_run_id,
            run_ts=run_ts,
            postgres_manifests=postgres_manifests,
            file_manifests=file_manifests,
        )

    postgres_rows = result["postgres"]["loaded_rows"]
    file_rows = result["files"]["loaded_rows"]
    logger.info(
        f"LOAD ONLY COMPLETE: run_id={effective_run_id} "
        f"postgres_loaded_rows={postgres_rows} file_loaded_rows={file_rows}"
    )
    return {"run_id": effective_run_id, "load": result}


if __name__ == "__main__":
    run_load_only()
