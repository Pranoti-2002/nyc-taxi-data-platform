"""Parquet conversion helpers for the Hive 3 runtime."""

import logging
from pathlib import Path
import tempfile

try:
    import pyarrow.parquet as pq
except ModuleNotFoundError:  # pragma: no cover - caller gets an explicit error
    pq = None


logger = logging.getLogger(__name__)


def convert_parquet_for_hive(local_path: Path) -> Path:
    """Rewrite a Parquet file with INT96 timestamps for Hive 3 compatibility."""
    if pq is None:
        raise ModuleNotFoundError(
            "pyarrow is required to convert Parquet files for Hive"
        )

    local_path = Path(local_path)
    with tempfile.NamedTemporaryFile(
        prefix=f"{local_path.stem}-hive-",
        suffix=".parquet",
        dir=local_path.parent,
        delete=False,
    ) as temporary_file:
        converted_path = Path(temporary_file.name)

    try:
        parquet_file = pq.ParquetFile(local_path)
        source_row_count = parquet_file.metadata.num_rows

        with pq.ParquetWriter(
            converted_path,
            parquet_file.schema_arrow,
            version="1.0",
            compression="snappy",
            use_deprecated_int96_timestamps=True,
        ) as writer:
            for batch in parquet_file.iter_batches(batch_size=65536):
                writer.write_batch(batch)

        converted_row_count = pq.ParquetFile(converted_path).metadata.num_rows
        if converted_row_count != source_row_count:
            raise ValueError(
                f"Parquet conversion changed the row count for {local_path}: "
                f"{source_row_count} -> {converted_row_count}"
            )
    except Exception:
        converted_path.unlink(missing_ok=True)
        raise

    logger.info(
        "Converted Parquet for Hive compatibility: %s (%d rows)",
        converted_path,
        source_row_count,
    )
    return converted_path
