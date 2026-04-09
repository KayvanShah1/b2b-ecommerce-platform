from __future__ import annotations

import json

from b2b_ec_pipeline.state.common import logger, to_json_safe
from b2b_ec_pipeline.state.models import DatasetSchemaSnapshot, IngestionCheckpoint, RunManifest, Watermark
from b2b_ec_utils.storage import storage


class MetadataArchiveWriter:
    def __init__(self, enabled: bool = True, archive_checkpoints: bool = False):
        self.enabled = enabled
        self.archive_checkpoints = archive_checkpoints

    def _write(self, path: str, payload: dict[str, object]) -> str | None:
        if not self.enabled:
            return None
        try:
            with storage.open(path, "wb") as handle:
                handle.write(json.dumps(to_json_safe(payload), ensure_ascii=False).encode("utf-8"))
            return path
        except Exception as exc:
            logger.warning(f"METADATA ARCHIVE WRITE FAILED: path={path} error={exc}")
            return None

    def archive_watermark(self, watermark: Watermark) -> str | None:
        path = storage.get_metadata_path(
            "watermarks",
            f"source={watermark.source}",
            f"dataset={watermark.dataset}",
            f"stage={watermark.stage}",
            f"run_date={watermark.updated_at.strftime('%Y-%m-%d')}",
            f"run_hour={watermark.updated_at.strftime('%H')}",
            f"run_id={watermark.run_id}.json",
        )
        return self._write(path, watermark.model_dump(mode="json"))

    def archive_run(self, manifest: RunManifest) -> str | None:
        path = storage.get_metadata_path(
            "runs",
            f"source={manifest.source}",
            f"dataset={manifest.dataset}",
            f"stage={manifest.stage}",
            f"run_date={manifest.run_ts.strftime('%Y-%m-%d')}",
            f"run_hour={manifest.run_ts.strftime('%H')}",
            f"run_id={manifest.run_id}.json",
        )
        return self._write(path, manifest.model_dump(mode="json"))

    def archive_checkpoint(self, checkpoint: IngestionCheckpoint) -> str | None:
        if not self.archive_checkpoints:
            return None
        path = storage.get_metadata_path(
            "lineage",
            "type=checkpoint",
            f"source={checkpoint.source}",
            f"dataset={checkpoint.dataset}",
            f"stage={checkpoint.stage}",
            f"run_date={checkpoint.updated_at.strftime('%Y-%m-%d')}",
            f"run_hour={checkpoint.updated_at.strftime('%H')}",
            f"run_id={checkpoint.run_id}",
            f"checkpoint={checkpoint.checkpoint_name}.json",
        )
        return self._write(path, checkpoint.model_dump(mode="json"))

    def archive_snapshot(self, snapshot: DatasetSchemaSnapshot) -> str | None:
        path = storage.get_metadata_path(
            "lineage",
            "type=schema_snapshot",
            f"source={snapshot.source}",
            f"dataset={snapshot.dataset}",
            f"stage={snapshot.stage}",
            f"run_date={snapshot.captured_at.strftime('%Y-%m-%d')}",
            f"run_hour={snapshot.captured_at.strftime('%H')}",
            f"run_id={snapshot.run_id}.json",
        )
        return self._write(path, snapshot.model_dump(mode="json"))
