import argparse
import logging
from pathlib import Path

import pyarrow.parquet as pq


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def inspect_parquet_schema(file_path: Path) -> None:
    """
    Read and display the schema of a Parquet file.
    """

    if not file_path.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {file_path}"
        )

    if file_path.suffix.lower() != ".parquet":
        raise ValueError(
            f"Expected a .parquet file, but received: {file_path}"
        )

    logger.info("Reading Parquet schema from: %s", file_path)

    parquet_file = pq.ParquetFile(file_path)

    schema = parquet_file.schema_arrow

    print("\nParquet Schema")
    print("=" * 80)
    print(f"File: {file_path}")
    print("=" * 80)

    print(
        f"{'Column Name':<35}"
        f"{'Parquet Type':<30}"
        f"{'Nullable':<10}"
    )

    print("-" * 80)

    for field in schema:
        print(
            f"{field.name:<35}"
            f"{str(field.type):<30}"
            f"{str(field.nullable):<10}"
        )

    print("-" * 80)
    print(f"Total columns: {len(schema)}")
    print("=" * 80)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect the schema of a Parquet file"
    )

    parser.add_argument(
        "parquet_file",
        type=Path,
        help="Path to the Parquet file",
    )

    args = parser.parse_args()

    try:
        inspect_parquet_schema(args.parquet_file)

    except Exception as exc:
        logger.error("Schema inspection failed: %s", exc)
        raise


if __name__ == "__main__":
    main()