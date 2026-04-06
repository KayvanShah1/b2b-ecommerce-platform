from datetime import datetime
from pathlib import Path

from b2b_ec_pipeline.ingestion import (
    POSTGRES_TABLE_CONFIGS,
    extract_postgres_table_to_raw,
    ingest_file_source_to_raw,
    load_file_manifests_to_staging,
    load_postgres_manifests_to_staging,
    process_file_dataset_to_processed,
    process_postgres_dataset_to_processed,
)
from b2b_ec_pipeline.ingestion.models import (
    FILE_PROCESS_SPECS,
    FILE_RAW_CAPTURE_SPECS,
    FileLoadResult,
    LoadBundle,
    ManifestBundle,
    PostgresLoadResult,
)
from b2b_ec_pipeline.state import RunManifest, Watermark, state_manager, state_resolver
from b2b_ec_utils import settings
from b2b_ec_utils.logger import get_logger
from dagster import ConfigurableResource
from dagster_duckdb import DuckDBResource
from pydantic import Field

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
        self,
        source: str,
        dataset: str,
        stage: str,
        run_id: str,
        run_ts: datetime,
        error_message: str,
    ) -> RunManifest:
        return RunManifest(
            source=source,
            dataset=dataset,
            stage=stage,
            run_id=run_id,
            run_ts=run_ts,
            status="failed",
            record_count=0,
            bad_record_count=0,
            raw_paths=[],
            processed_paths=[],
            processed_files=[],
            error_message=error_message,
        )

    def _handle_step_exception(
        self,
        *,
        stage: str,
        source: str,
        dataset: str,
        run_id: str,
        run_ts: datetime,
        exc: Exception,
    ) -> RunManifest:
        logger.exception(f"{stage.upper()} FAILED: source={source} dataset={dataset} run_id={run_id}: {exc}")
        persisted = state_manager.get_run_manifest(run_id=run_id, source=source, dataset=dataset, stage=stage)
        if persisted is not None:
            return persisted
        return self._failed_manifest(
            source=source,
            dataset=dataset,
            stage=stage,
            run_id=run_id,
            run_ts=run_ts,
            error_message=str(exc),
        )

    def _commit_stage_watermarks(self, manifests: list[RunManifest]) -> None:
        for manifest in manifests:
            watermark_payload = manifest.watermark_after
            if not watermark_payload:
                continue
            state_manager.put_watermark(Watermark.model_validate(watermark_payload))

    def _latest_manifest_for_stage(self, source: str, dataset: str, stage: str) -> RunManifest | None:
        return state_resolver.latest_completed_manifest(source=source, dataset=dataset, stage=stage)

    def _resolve_raw_result(self, raw_result: ManifestBundle | None) -> ManifestBundle:
        if raw_result is not None:
            return raw_result

        postgres_manifests: list[RunManifest] = []
        for table_cfg in self._selected_postgres_configs():
            manifest = self._latest_manifest_for_stage(source="postgres", dataset=table_cfg.name, stage="raw_capture")
            if manifest:
                postgres_manifests.append(manifest)

        file_manifests: dict[str, RunManifest] = {}
        for dataset_key, spec in FILE_RAW_CAPTURE_SPECS.items():
            manifest = self._latest_manifest_for_stage(source=spec.source, dataset=spec.dataset, stage="raw_capture")
            if manifest:
                file_manifests[dataset_key] = manifest

        return ManifestBundle(postgres=postgres_manifests, files=file_manifests)

    def _resolve_process_manifests(
        self,
        postgres_manifests: list[RunManifest] | None,
        file_manifests: dict[str, RunManifest] | None,
    ) -> tuple[list[RunManifest], dict[str, RunManifest]]:
        logger.info("LOAD RESOLVE: resolving process manifests")
        if postgres_manifests is None:
            postgres_manifests = []
            for table_cfg in self._selected_postgres_configs():
                manifest = self._latest_manifest_for_stage(source="postgres", dataset=table_cfg.name, stage="process")
                if manifest:
                    postgres_manifests.append(manifest)
                    logger.info(
                        f"LOAD RESOLVE: source=postgres dataset={table_cfg.name} "
                        f"manifest_run_id={manifest.run_id} status={manifest.status}"
                    )
                else:
                    logger.warning(
                        f"LOAD RESOLVE: source=postgres dataset={table_cfg.name} no completed process manifest"
                    )

        if file_manifests is None:
            file_manifests = {}
            for dataset_key, spec in FILE_PROCESS_SPECS.items():
                manifest = self._latest_manifest_for_stage(source=spec.source, dataset=spec.dataset, stage="process")
                if manifest:
                    file_manifests[dataset_key] = manifest
                    logger.info(
                        f"LOAD RESOLVE: source={spec.source} dataset={spec.dataset} "
                        f"manifest_run_id={manifest.run_id} status={manifest.status}"
                    )
                else:
                    logger.warning(
                        f"LOAD RESOLVE: source={spec.source} dataset={spec.dataset} no completed process manifest"
                    )

        return postgres_manifests, file_manifests

    def resolve_process_manifests(
        self,
        postgres_manifests: list[RunManifest] | None = None,
        file_manifests: dict[str, RunManifest] | None = None,
    ) -> tuple[list[RunManifest], dict[str, RunManifest]]:
        return self._resolve_process_manifests(postgres_manifests, file_manifests)

    def raw_capture_postgres(
        self,
        run_id: str,
        run_ts: datetime,
        commit_watermarks: bool = True,
    ) -> list[RunManifest]:
        manifests: list[RunManifest] = []
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
    ) -> dict[str, RunManifest]:
        manifests: dict[str, RunManifest] = {}
        for dataset_key, spec in FILE_RAW_CAPTURE_SPECS.items():
            try:
                manifests[dataset_key] = ingest_file_source_to_raw(
                    dataset_key=dataset_key,
                    run_id=run_id,
                    run_ts=run_ts,
                )
            except Exception as exc:
                manifests[dataset_key] = self._handle_step_exception(
                    stage="raw_capture",
                    source=spec.source,
                    dataset=spec.dataset,
                    run_id=run_id,
                    run_ts=run_ts,
                    exc=exc,
                )
                if self.fail_fast:
                    raise

        if commit_watermarks:
            self._commit_stage_watermarks(list(manifests.values()))
        return manifests

    def raw_capture_all(
        self,
        run_id: str,
        run_ts: datetime,
        commit_watermarks: bool = True,
    ) -> ManifestBundle:
        # Avoid duplicate commits by delegating commit only in this method.
        postgres_manifests = self.raw_capture_postgres(run_id=run_id, run_ts=run_ts, commit_watermarks=False)
        file_manifests = self.raw_capture_files(run_id=run_id, run_ts=run_ts, commit_watermarks=False)
        if commit_watermarks:
            self._commit_stage_watermarks(postgres_manifests + list(file_manifests.values()))
        return ManifestBundle(postgres=postgres_manifests, files=file_manifests)

    def process_postgres_manifests(
        self,
        run_id: str,
        run_ts: datetime,
        raw_manifests: list[RunManifest] | None = None,
        commit_watermarks: bool = True,
    ) -> list[RunManifest]:
        raw_manifest_by_dataset = {manifest.dataset: manifest for manifest in (raw_manifests or [])}
        processed_manifests: list[RunManifest] = []

        for table_cfg in self._selected_postgres_configs():
            raw_manifest = raw_manifest_by_dataset.get(table_cfg.name)
            try:
                processed_manifests.append(
                    process_postgres_dataset_to_processed(
                        table_cfg=table_cfg,
                        run_id=run_id,
                        run_ts=run_ts,
                        hint_raw_paths=raw_manifest.raw_paths if raw_manifest else [],
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
        raw_file_manifests: dict[str, RunManifest] | None = None,
        commit_watermarks: bool = True,
    ) -> dict[str, RunManifest]:
        raw_file_manifests = raw_file_manifests or {}
        manifests: dict[str, RunManifest] = {}
        for dataset_key, spec in FILE_PROCESS_SPECS.items():
            raw_manifest = raw_file_manifests.get(dataset_key)
            hint_raw_paths = raw_manifest.raw_paths if raw_manifest else []
            try:
                manifests[dataset_key] = process_file_dataset_to_processed(
                    dataset_key=dataset_key,
                    run_id=run_id,
                    run_ts=run_ts,
                    hint_raw_paths=hint_raw_paths,
                )
            except Exception as exc:
                manifests[dataset_key] = self._handle_step_exception(
                    stage="process",
                    source=spec.source,
                    dataset=spec.dataset,
                    run_id=run_id,
                    run_ts=run_ts,
                    exc=exc,
                )
                if self.fail_fast:
                    raise

        if commit_watermarks:
            self._commit_stage_watermarks(list(manifests.values()))
        return manifests

    def process_all(
        self,
        run_id: str,
        run_ts: datetime,
        raw_result: ManifestBundle | None = None,
        commit_watermarks: bool = True,
    ) -> ManifestBundle:
        resolved_raw_result = self._resolve_raw_result(raw_result)
        postgres_manifests = self.process_postgres_manifests(
            run_id=run_id,
            run_ts=run_ts,
            raw_manifests=resolved_raw_result.postgres,
            commit_watermarks=False,
        )
        file_manifests = self.process_file_manifests(
            run_id=run_id,
            run_ts=run_ts,
            raw_file_manifests=resolved_raw_result.files,
            commit_watermarks=False,
        )
        if commit_watermarks:
            self._commit_stage_watermarks(postgres_manifests + list(file_manifests.values()))
        return ManifestBundle(postgres=postgres_manifests, files=file_manifests)

    def load_all_to_staging(
        self,
        conn,
        run_id: str,
        run_ts: datetime,
        postgres_manifests: list[RunManifest] | None = None,
        file_manifests: dict[str, RunManifest] | None = None,
        commit_watermarks: bool = True,
    ) -> LoadBundle:
        logger.info(f"LOAD START: run_id={run_id}")
        postgres_manifests, file_manifests = self._resolve_process_manifests(postgres_manifests, file_manifests)
        logger.info(
            f"LOAD INPUT: run_id={run_id} postgres_manifests={len(postgres_manifests)} "
            f"file_manifests={len(file_manifests)}"
        )
        if not postgres_manifests and not file_manifests:
            logger.warning(f"LOAD SKIP: run_id={run_id} no process manifests available")
            return LoadBundle(postgres=PostgresLoadResult(), files=FileLoadResult())
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
            file_manifests=file_manifests,
        )

        if commit_watermarks:
            self._commit_stage_watermarks(postgres_result.manifests)
            self._commit_stage_watermarks(list(files_result.manifests.values()))

        logger.info(
            f"LOAD COMPLETE: run_id={run_id} postgres_loaded_rows={postgres_result.loaded_rows} "
            f"file_loaded_rows={files_result.loaded_rows}"
        )
        return LoadBundle(postgres=postgres_result, files=files_result)


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
