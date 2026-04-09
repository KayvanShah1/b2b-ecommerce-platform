from collections.abc import Callable

import polars as pl
from b2b_ec_utils.storage import storage


def read_parquet_frame(path: str) -> pl.DataFrame:
    with storage.open(path, mode="rb") as file_handle:
        return pl.read_parquet(file_handle)


def write_parquet_frame(path: str, dataframe: pl.DataFrame) -> int:
    with storage.open(path, mode="wb") as file_handle:
        dataframe.write_parquet(file_handle)
    return dataframe.height


def schema_columns(dataframe: pl.DataFrame) -> list[dict[str, object]]:
    return [{"name": name, "dtype": str(dtype), "nullable": True} for name, dtype in dataframe.schema.items()]


def write_parquet_chunks(
    *,
    dataframe: pl.DataFrame,
    chunk_size: int,
    output_path_for_chunk: Callable[[int], str],
) -> tuple[list[str], int]:
    if dataframe.is_empty():
        return [], 0

    output_paths: list[str] = []
    total_rows = 0
    chunk_index = 0

    for start in range(0, dataframe.height, chunk_size):
        chunk = dataframe.slice(start, chunk_size)
        output_path = output_path_for_chunk(chunk_index)
        write_parquet_frame(output_path, chunk)
        output_paths.append(output_path)
        total_rows += chunk.height
        chunk_index += 1

    return output_paths, total_rows
