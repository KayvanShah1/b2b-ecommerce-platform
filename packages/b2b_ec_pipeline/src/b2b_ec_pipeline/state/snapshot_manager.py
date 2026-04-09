from __future__ import annotations

from psycopg2 import sql
from psycopg2.extras import Json

from b2b_ec_pipeline.state.archive import MetadataArchiveWriter
from b2b_ec_pipeline.state.bootstrap import ensure_etl_metadata_schema
from b2b_ec_pipeline.state.common import ETL_METADATA_SCHEMA, state_ref, to_json_safe
from b2b_ec_pipeline.state.models import DatasetSchemaSnapshot, IngestionCheckpoint
from b2b_ec_sources import get_connection


class IngestionSnapshotManager:
    def __init__(self, schema: str = ETL_METADATA_SCHEMA, archive_writer: MetadataArchiveWriter | None = None):
        self.schema = schema
        self.archive_writer = archive_writer or MetadataArchiveWriter()

    def put_checkpoint(self, checkpoint: IngestionCheckpoint) -> str:
        ensure_etl_metadata_schema(self.schema)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.checkpoints (
                            run_id, source, dataset, stage, checkpoint_name, checkpoint_value, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_id, source, dataset, stage, checkpoint_name)
                        DO UPDATE SET checkpoint_value = EXCLUDED.checkpoint_value, updated_at = EXCLUDED.updated_at
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (
                        checkpoint.run_id,
                        str(checkpoint.source),
                        checkpoint.dataset,
                        str(checkpoint.stage),
                        checkpoint.checkpoint_name,
                        Json(to_json_safe(checkpoint.checkpoint_value)),
                        checkpoint.updated_at,
                    ),
                )
        self.archive_writer.archive_checkpoint(checkpoint)
        return state_ref(
            self.schema,
            "checkpoints",
            checkpoint.run_id,
            str(checkpoint.source),
            checkpoint.dataset,
            str(checkpoint.stage),
            checkpoint.checkpoint_name,
        )

    def put_dataset_schema_snapshot(self, snapshot: DatasetSchemaSnapshot) -> str:
        ensure_etl_metadata_schema(self.schema)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                        INSERT INTO {schema}.schema_snapshots (
                            run_id, source, dataset, stage, captured_at, row_count, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (run_id, source, dataset, stage)
                        DO UPDATE SET captured_at = EXCLUDED.captured_at, row_count = EXCLUDED.row_count, updated_at = NOW()
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (
                        snapshot.run_id,
                        str(snapshot.source),
                        snapshot.dataset,
                        str(snapshot.stage),
                        snapshot.captured_at,
                        snapshot.row_count,
                    ),
                )
                cur.execute(
                    sql.SQL(
                        """
                        DELETE FROM {schema}.schema_snapshot_columns
                        WHERE run_id = %s AND source = %s AND dataset = %s AND stage = %s
                        """
                    ).format(schema=sql.Identifier(self.schema)),
                    (snapshot.run_id, str(snapshot.source), snapshot.dataset, str(snapshot.stage)),
                )

                column_rows = [
                    (
                        snapshot.run_id,
                        str(snapshot.source),
                        snapshot.dataset,
                        str(snapshot.stage),
                        column_index,
                        column.name,
                        column.dtype,
                        column.nullable,
                    )
                    for column_index, column in enumerate(snapshot.columns)
                ]
                if column_rows:
                    cur.executemany(
                        sql.SQL(
                            """
                            INSERT INTO {schema}.schema_snapshot_columns (
                                run_id, source, dataset, stage, column_index, name, dtype, nullable, updated_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                            """
                        ).format(schema=sql.Identifier(self.schema)),
                        column_rows,
                    )

        self.archive_writer.archive_snapshot(snapshot)
        return state_ref(
            self.schema,
            "schema_snapshots",
            snapshot.run_id,
            str(snapshot.source),
            snapshot.dataset,
            str(snapshot.stage),
        )
