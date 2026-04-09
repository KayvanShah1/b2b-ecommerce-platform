from __future__ import annotations

import re
from pathlib import Path
from threading import Lock

from jinja2 import Environment, StrictUndefined
from psycopg2 import sql

from b2b_ec_pipeline.state.common import ETL_METADATA_SCHEMA
from b2b_ec_sources import get_connection

_bootstrap_lock = Lock()
_bootstrapped_schemas: set[str] = set()

_SQL_DIR = Path(__file__).resolve().parent / "sql"
_SCHEMA_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_CASCADED_CHILD_TABLES = (
    "schema_snapshot_columns",
    "schema_snapshots",
    "checkpoints",
    "run_paths",
)


def _validate_schema_name(schema: str) -> str:
    if not _SCHEMA_IDENTIFIER_RE.fullmatch(schema):
        raise ValueError(f"Invalid schema identifier: {schema}")
    return schema


def _render_sql_template(template_name: str, **context: object) -> str:
    template_path = _SQL_DIR / template_name
    template = Environment(undefined=StrictUndefined, autoescape=False).from_string(
        template_path.read_text(encoding="utf-8")
    )
    return template.render(**context)


def _table_has_column(cur, schema: str, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """,
        (schema, table, column),
    )
    return cur.fetchone() is not None


def _validate_layout(cur, schema: str) -> None:
    required = {
        "watermarks": ("source", "dataset", "stage", "watermark_value", "run_id"),
        "runs": ("run_id", "source", "dataset", "stage", "status", "run_ts", "watermark_before", "watermark_after"),
        "run_paths": ("run_id", "source", "dataset", "stage", "path_kind", "path_index", "path"),
        "checkpoints": ("run_id", "source", "dataset", "stage", "checkpoint_name", "checkpoint_value"),
        "schema_snapshots": ("run_id", "source", "dataset", "stage", "captured_at"),
        "schema_snapshot_columns": ("run_id", "source", "dataset", "stage", "column_index", "name", "dtype"),
    }
    for table, columns in required.items():
        for column in columns:
            if not _table_has_column(cur, schema, table, column):
                raise RuntimeError(
                    f"etl_metadata schema is invalid (missing {table}.{column}). "
                    "Run full reset to recreate etl metadata schema."
                )


def _ensure_schema_bootstrapped(schema: str) -> None:
    validated_schema = _validate_schema_name(schema)
    with _bootstrap_lock:
        if validated_schema in _bootstrapped_schemas:
            return

        ddl_sql = _render_sql_template("etl_metadata.sql.j2", schema=validated_schema)
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl_sql)
                _validate_layout(cur, validated_schema)

        _bootstrapped_schemas.add(validated_schema)


def ensure_etl_metadata_schema(schema: str = ETL_METADATA_SCHEMA) -> None:
    _ensure_schema_bootstrapped(schema)


def clear_etl_metadata_for_stages(stages: list[str], schema: str = ETL_METADATA_SCHEMA) -> dict[str, int]:
    _ensure_schema_bootstrapped(schema)
    stage_values = sorted(set(stages))
    deleted: dict[str, int] = {table: 0 for table in (*_CASCADED_CHILD_TABLES, "runs", "watermarks")}

    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for table_name in _CASCADED_CHILD_TABLES:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {schema}.{table} WHERE stage = ANY(%s)").format(
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(table_name),
                    ),
                    (stage_values,),
                )
                deleted[table_name] = int(cur.fetchone()[0])

            # Child tables are removed through FK cascade on runs.
            cur.execute(
                sql.SQL("DELETE FROM {schema}.runs WHERE stage = ANY(%s)").format(schema=sql.Identifier(schema)),
                (stage_values,),
            )
            deleted["runs"] = cur.rowcount

            cur.execute(
                sql.SQL("DELETE FROM {schema}.watermarks WHERE stage = ANY(%s)").format(schema=sql.Identifier(schema)),
                (stage_values,),
            )
            deleted["watermarks"] = cur.rowcount

    return deleted


def recreate_etl_metadata_schema(schema: str = ETL_METADATA_SCHEMA) -> None:
    with get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {schema} CASCADE").format(schema=sql.Identifier(schema)))

    with _bootstrap_lock:
        _bootstrapped_schemas.discard(schema)
    _ensure_schema_bootstrapped(schema)
