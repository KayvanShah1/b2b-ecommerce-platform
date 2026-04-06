from datetime import datetime
from pathlib import Path
from typing import Any

from b2b_ec_utils import settings
from b2b_ec_utils.logger import get_logger
from dagster import ConfigurableResource
from dagster_duckdb import DuckDBResource
from pydantic import Field

from b2b_ec_etl.defs.ingestion.core import (
    POSTGRES_TABLE_CONFIGS,
    extract_postgres_table_to_raw,
    ingest_marketing_leads_to_raw,
    ingest_web_logs_to_raw,
    load_file_manifests_to_staging,
    load_postgres_manifests_to_staging,
    process_marketing_to_processed,
    process_postgres_dataset_to_processed,
    process_web_logs_to_processed,
)
from b2b_ec_etl.defs.ingestion.core.models import Watermark
from b2b_ec_etl.defs.ingestion.core.state import state_manager, state_resolver

logger = get_logger("IngestionResource")


class IngestionResource(ConfigurableResource):
    include_postgres_tables: list[str] = Field(default_factory=list)
    fail_fast: bool = False

    def _selected_postgres_configs(self):
        if not self.include_postgres_tables:
            return POSTGRES_TABLE_CONFIGS

        include = set(self.include_postgres_tables)
        return tuple(cfg for cfg in POSTGRES_TABLE_CONFIGS if cfg.name in include)

    def _failed_manifest(
        self, source: str, dataset: str, run_id: str, run_ts: datetime, error_message: str
    ) -> dict[str, Any]:
        return {
            "source": source,
            "dataset": dataset,
            "run_id": run_id,
            "run_ts": run_ts.isoformat(),
            "status": "failed",
            "record_count": 0,
            "bad_record_count": 0,
            "bad_line_count": 0,
            "raw_paths": [],
            "processed_paths": [],
            "processed_files": [],
            "error_message": error_message,
        }

    def _handle_step_exception(
        self,
        *,
        stage: str,
        source: str,
        dataset: str,
        run_id: str,
        run_ts: datetime,
        exc: Exception,
    ) -> dict[str, Any]:
        logger.exception(f"{stage.upper()} FAILED: source={source} dataset={dataset} run_id={run_id}: {exc}")
        persisted = state_manager.get_run_manifest(run_id=run_id, source=source, dataset=dataset, stage=stage)
        if persisted is not None:
            return persisted.model_dump(mode="json")
        return self._failed_manifest(
            source=source,
            dataset=dataset,
            run_id=run_id,
            run_ts=run_ts,
            error_message=str(exc),
        )

    def _commit_stage_watermarks(self, manifests: list[dict[str, Any]]) -> None:
        for manifest in manifests:
            watermark_payload = manifest.get("watermark_after")
            if not watermark_payload:
                continue
            state_manager.put_watermark(Watermark.model_validate(watermark_payload))

    def _latest_manifest_for_stage(self, source: str, dataset: str, stage: str) -> dict[str, Any] | None:
        manifest = state_resolver.latest_completed_manifest(source=source, dataset=dataset, stage=stage)
        return manifest.model_dump(mode="json") if manifest else None

    def _resolve_raw_result(self, raw_result: dict[str, Any] | None) -> dict[str, Any]:
        if raw_result is not None:
            return raw_result

        postgres_manifests: list[dict[str, Any]] = []
        for table_cfg in self._selected_postgres_configs():
            manifest = self._latest_manifest_for_stage(source="postgres", dataset=table_cfg.name, stage="raw_capture")
            if manifest:
                postgres_manifests.append(manifest)

        file_manifests: dict[str, dict[str, Any]] = {}
        for source_name, dataset in [("marketing_leads", "marketing_leads"), ("webserver_logs", "webserver_logs")]:
            manifest = self._latest_manifest_for_stage(source=source_name, dataset=dataset, stage="raw_capture")
            if manifest:
                file_manifests[source_name] = manifest

        return {"postgres": postgres_manifests, "files": file_manifests}

    def _resolve_process_manifests(
        self,
        postgres_manifests: list[dict[str, Any]] | None,
        file_manifests: dict[str, dict[str, Any]] | None,
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        logger.info("LOAD RESOLVE: resolving process manifests")
        if postgres_manifests is None:
            postgres_manifests = []
            for table_cfg in self._selected_postgres_configs():
                manifest = self._latest_manifest_for_stage(source="postgres", dataset=table_cfg.name, stage="process")
                if manifest:
                    postgres_manifests.append(manifest)
                    logger.info(
                        f"LOAD RESOLVE: source=postgres dataset={table_cfg.name} "
                        f"manifest_run_id={manifest.get('run_id')} status={manifest.get('status')}"
                    )
                else:
                    logger.warning(
                        f"LOAD RESOLVE: source=postgres dataset={table_cfg.name} no completed process manifest"
                    )

        if file_manifests is None:
            file_manifests = {}
            for source_name, dataset in [("marketing_leads", "marketing_leads"), ("webserver_logs", "webserver_logs")]:
                manifest = self._latest_manifest_for_stage(source=source_name, dataset=dataset, stage="process")
                if manifest:
                    file_manifests[source_name] = manifest
                    logger.info(
                        f"LOAD RESOLVE: source={source_name} dataset={dataset} "
                        f"manifest_run_id={manifest.get('run_id')} status={manifest.get('status')}"
                    )
                else:
                    logger.warning(
                        f"LOAD RESOLVE: source={source_name} dataset={dataset} no completed process manifest"
                    )

        return postgres_manifests, file_manifests

    def resolve_process_manifests(
        self,
        postgres_manifests: list[dict[str, Any]] | None = None,
        file_manifests: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        return self._resolve_process_manifests(postgres_manifests, file_manifests)

    def raw_capture_postgres(
        self,
        run_id: str,
        run_ts: datetime,
        commit_watermarks: bool = True,
    ) -> list[dict[str, Any]]:
        manifests: list[dict[str, Any]] = []
        for table_cfg in self._selected_postgres_configs():
            try:
                manifests.append(extract_postgres_table_to_raw(table_cfg, run_id=run_id, run_ts=run_ts))
            except Exception as exc:
                manifests.append(
                    self._handle_step_exception(
                        stage="raw_capture",
                        source="postgres",
                        dataset=table_cfg.name,
                        run_id=run_id,
                        run_ts=run_ts,
                        exc=exc,
                    )
                )
                if self.fail_fast:
                    raise
        if commit_watermarks:
            self._commit_stage_watermarks(manifests)
        return manifests

    def raw_capture_files(
        self,
        run_id: str,
        run_ts: datetime,
        commit_watermarks: bool = True,
    ) -> dict[str, dict[str, Any]]:
        try:
            marketing_manifest = ingest_marketing_leads_to_raw(run_id=run_id, run_ts=run_ts)
        except Exception as exc:
            marketing_manifest = self._handle_step_exception(
                stage="raw_capture",
                source="marketing_leads",
                dataset="marketing_leads",
                run_id=run_id,
                run_ts=run_ts,
                exc=exc,
            )
            if self.fail_fast:
                raise

        try:
            web_logs_manifest = ingest_web_logs_to_raw(run_id=run_id, run_ts=run_ts)
        except Exception as exc:
            web_logs_manifest = self._handle_step_exception(
                stage="raw_capture",
                source="webserver_logs",
                dataset="webserver_logs",
                run_id=run_id,
                run_ts=run_ts,
                exc=exc,
            )
            if self.fail_fast:
                raise

        manifests = {"marketing_leads": marketing_manifest, "webserver_logs": web_logs_manifest}
        if commit_watermarks:
            self._commit_stage_watermarks(list(manifests.values()))
        return manifests

    def raw_capture_all(
        self,
        run_id: str,
        run_ts: datetime,
        commit_watermarks: bool = True,
    ) -> dict[str, Any]:
        # Avoid duplicate commits by delegating commit only in this method.
        postgres_manifests = self.raw_capture_postgres(run_id=run_id, run_ts=run_ts, commit_watermarks=False)
        file_manifests = self.raw_capture_files(run_id=run_id, run_ts=run_ts, commit_watermarks=False)
        if commit_watermarks:
            self._commit_stage_watermarks(postgres_manifests + list(file_manifests.values()))
        return {"postgres": postgres_manifests, "files": file_manifests}

    def process_postgres_manifests(
        self,
        run_id: str,
        run_ts: datetime,
        raw_manifests: list[dict[str, Any]] | None = None,
        commit_watermarks: bool = True,
    ) -> list[dict[str, Any]]:
        raw_manifest_by_dataset = {manifest["dataset"]: manifest for manifest in (raw_manifests or [])}
        processed_manifests: list[dict[str, Any]] = []

        for table_cfg in self._selected_postgres_configs():
            try:
                processed_manifests.append(
                    process_postgres_dataset_to_processed(
                        table_cfg=table_cfg,
                        run_id=run_id,
                        run_ts=run_ts,
                        hint_raw_paths=(raw_manifest_by_dataset.get(table_cfg.name) or {}).get("raw_paths", []),
                    )
                )
            except Exception as exc:
                processed_manifests.append(
                    self._handle_step_exception(
                        stage="process",
                        source="postgres",
                        dataset=table_cfg.name,
                        run_id=run_id,
                        run_ts=run_ts,
                        exc=exc,
                    )
                )
                if self.fail_fast:
                    raise

        if commit_watermarks:
            self._commit_stage_watermarks(processed_manifests)
        return processed_manifests

    def process_file_manifests(
        self,
        run_id: str,
        run_ts: datetime,
        raw_file_manifests: dict[str, dict[str, Any]] | None = None,
        commit_watermarks: bool = True,
    ) -> dict[str, dict[str, Any]]:
        raw_file_manifests = raw_file_manifests or {}
        try:
            marketing_manifest = process_marketing_to_processed(
                run_id=run_id,
                run_ts=run_ts,
                hint_raw_paths=(raw_file_manifests.get("marketing_leads") or {}).get("raw_paths", []),
            )
        except Exception as exc:
            marketing_manifest = self._handle_step_exception(
                stage="process",
                source="marketing_leads",
                dataset="marketing_leads",
                run_id=run_id,
                run_ts=run_ts,
                exc=exc,
            )
            if self.fail_fast:
                raise

        try:
            web_logs_manifest = process_web_logs_to_processed(
                run_id=run_id,
                run_ts=run_ts,
                hint_raw_paths=(raw_file_manifests.get("webserver_logs") or {}).get("raw_paths", []),
            )
        except Exception as exc:
            web_logs_manifest = self._handle_step_exception(
                stage="process",
                source="webserver_logs",
                dataset="webserver_logs",
                run_id=run_id,
                run_ts=run_ts,
                exc=exc,
            )
            if self.fail_fast:
                raise
        manifests = {"marketing_leads": marketing_manifest, "webserver_logs": web_logs_manifest}
        if commit_watermarks:
            self._commit_stage_watermarks(list(manifests.values()))
        return manifests

    def process_all(
        self,
        run_id: str,
        run_ts: datetime,
        raw_result: dict[str, Any] | None = None,
        commit_watermarks: bool = True,
    ) -> dict[str, Any]:
        resolved_raw_result = self._resolve_raw_result(raw_result)
        postgres_manifests = self.process_postgres_manifests(
            run_id=run_id,
            run_ts=run_ts,
            raw_manifests=resolved_raw_result.get("postgres", []),
            commit_watermarks=False,
        )
        file_manifests = self.process_file_manifests(
            run_id=run_id,
            run_ts=run_ts,
            raw_file_manifests=resolved_raw_result.get("files", {}),
            commit_watermarks=False,
        )
        if commit_watermarks:
            self._commit_stage_watermarks(postgres_manifests + list(file_manifests.values()))
        return {"postgres": postgres_manifests, "files": file_manifests}

    def load_all_to_staging(
        self,
        conn,
        run_id: str,
        run_ts: datetime,
        postgres_manifests: list[dict[str, Any]] | None = None,
        file_manifests: dict[str, dict[str, Any]] | None = None,
        commit_watermarks: bool = True,
    ) -> dict[str, Any]:
        logger.info(f"LOAD START: run_id={run_id}")
        postgres_manifests, file_manifests = self._resolve_process_manifests(postgres_manifests, file_manifests)
        logger.info(
            f"LOAD INPUT: run_id={run_id} postgres_manifests={len(postgres_manifests)} "
            f"file_manifests={len(file_manifests)}"
        )
        if not postgres_manifests and not file_manifests:
            logger.warning(f"LOAD SKIP: run_id={run_id} no process manifests available")
            return {
                "postgres": {"manifests": [], "loaded_rows": 0, "loaded_tables": []},
                "files": {"manifests": {}, "loaded_rows": 0, "loaded_tables": []},
            }
        selected_cfgs = self._selected_postgres_configs()
        postgres_result = load_postgres_manifests_to_staging(
            conn,
            selected_cfgs,
            run_id=run_id,
            run_ts=run_ts,
            manifests=postgres_manifests,
        )
        files_result = load_file_manifests_to_staging(
            conn,
            run_id=run_id,
            run_ts=run_ts,
            marketing_manifest=file_manifests.get("marketing_leads"),
            web_logs_manifest=file_manifests.get("webserver_logs"),
        )

        if commit_watermarks:
            self._commit_stage_watermarks(postgres_result.get("manifests", []))
            self._commit_stage_watermarks(list(files_result.get("manifests", {}).values()))

        logger.info(
            f"LOAD COMPLETE: run_id={run_id} postgres_loaded_rows={postgres_result.get('loaded_rows', 0)} "
            f"file_loaded_rows={files_result.get('loaded_rows', 0)}"
        )
        return {"postgres": postgres_result, "files": files_result}


def _resolve_local_duckdb_path() -> str:
    local_name = settings.motherduck.local_database
    local_path = Path(local_name)
    if not local_path.is_absolute():
        local_path = settings.local_warehouse_dir / local_name
    local_path.parent.mkdir(parents=True, exist_ok=True)
    return str(local_path)


def resolve_duckdb_database_uri(prefer_local: bool = False) -> str:
    if prefer_local:
        return _resolve_local_duckdb_path()

    token = settings.motherduck.token.get_secret_value() if settings.motherduck.token else None
    if token and token != "<API_TOKEN>":
        return f"md:{settings.motherduck.database}?motherduck_token={token}"

    return _resolve_local_duckdb_path()


ingestion_resources = {
    "ingestion_resource": IngestionResource(),
    "duckdb": DuckDBResource(database=resolve_duckdb_database_uri()),
}
