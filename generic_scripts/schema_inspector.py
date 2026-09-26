import argparse
import logging
from pathlib import Path
import csv
import pyarrow.parquet as pq


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def inspect_parquet_schema(file_path: Path) -> list[tuple[str, str, bool]]:
    """
    Read the schema of a Parquet file.

    Returns:
        A list containing:
        (column_name, parquet_type, nullable)
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

    return [
        (field.name, str(field.type), field.nullable)
        for field in schema
    ]


def format_schema_report(
    file_path: Path,
    schema: list[tuple[str, str, bool]],
) -> str:
    """
    Format one Parquet schema as readable text.
    """

    lines = []

    lines.append("=" * 80)
    lines.append(f"FILE: {file_path}")
    lines.append("=" * 80)
    lines.append("")

    lines.append(
        f"{'Column Name':<35}"
        f"{'Data Type':<30}"
        f"{'Nullable':<10}"
    )

    lines.append("-" * 80)

    for column_name, parquet_type, nullable in schema:
        lines.append(
            f"{column_name:<35}"
            f"{parquet_type:<30}"
            f"{str(nullable):<10}"
        )

    lines.append("-" * 80)
    lines.append(f"Total columns: {len(schema)}")
    lines.append("")

    return "\n".join(lines)


def write_schema_report(
    input_files: list[Path],
    output_file: Path,
) -> None:
    """
    Inspect multiple Parquet or CSV files and write
    their schemas into one text report.
    """

    logger.info(
        "Inspecting %d input files",
        len(input_files),
    )

    report_sections = []

    for input_file in input_files:

        if input_file.suffix.lower() == ".parquet":
            schema = inspect_parquet_schema(input_file)

        elif input_file.suffix.lower() == ".csv":
            schema = inspect_csv_schema(input_file)

        else:
            raise ValueError(
                f"Unsupported file type: {input_file.suffix}"
            )

        report = format_schema_report(
            file_path=input_file,
            schema=schema,
        )

        report_sections.append(report)

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file.write_text(
        "\n".join(report_sections),
        encoding="utf-8",
    )

    logger.info(
        "Schema report written to: %s",
        output_file,
    )

def infer_value_type(value: str) -> str:
    """
    Infer the most suitable type for one CSV value.
    """

    value = value.strip()

    if not value:
        return "NULL"

    try:
        int(value)
        return "INT"
    except ValueError:
        pass

    try:
        float(value)
        return "DOUBLE"
    except ValueError:
        pass

    if value.lower() in {"true", "false"}:
        return "BOOLEAN"

    return "STRING"

def merge_inferred_types(types: set[str]) -> str:
    """
    Determine one final type for a column based on
    the types observed across its values.
    """

    types.discard("NULL")

    if not types:
        return "STRING"

    if types == {"INT"}:
        return "INT"

    if types <= {"INT", "DOUBLE"}:
        return "DOUBLE" if "DOUBLE" in types else "INT"

    if types == {"BOOLEAN"}:
        return "BOOLEAN"

    return "STRING"

def inspect_csv_schema(file_path: Path) -> list[tuple[str, str, bool]]:
    """
    Inspect the schema of a CSV file by inferring column types.

    Returns:
        A list containing:
        (column_name, inferred_type, nullable)
    """

    if not file_path.exists():
        raise FileNotFoundError(
            f"CSV file not found: {file_path}"
        )

    if file_path.suffix.lower() != ".csv":
        raise ValueError(
            f"Expected a .csv file, but received: {file_path}"
        )

    logger.info("Reading CSV schema from: %s", file_path)

    with file_path.open(
        mode="r",
        encoding="utf-8",
        newline="",
    ) as csv_file:

        reader = csv.DictReader(csv_file)

        if reader.fieldnames is None:
            raise ValueError(
                f"CSV file does not contain a header: {file_path}"
            )

        column_types = {
            column_name: set()
            for column_name in reader.fieldnames
        }

        nullable_columns = {
            column_name: False
            for column_name in reader.fieldnames
        }

        for row in reader:
            for column_name in reader.fieldnames:

                value = row.get(column_name, "")

                if not value or not value.strip():
                    nullable_columns[column_name] = True

                inferred_type = infer_value_type(value)

                column_types[column_name].add(inferred_type)

    schema = []

    for column_name in reader.fieldnames:

        final_type = merge_inferred_types(
            column_types[column_name]
        )

        schema.append(
            (
                column_name,
                final_type,
                nullable_columns[column_name],
            )
        )

    return schema


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect schemas of multiple Parquet files"
    )

    parser.add_argument(
        "input_files",
        type=Path,
        nargs="+",
        help="One or more Parquet or CSV files to inspect",
    )

    parser.add_argument(
        "--output-file",
        type=Path,
        required=True,
        help="Path where the schema report should be written",
    )

    args = parser.parse_args()

    try:
        write_schema_report(
            input_files=args.input_files,
            output_file=args.output_file,
        )

    except Exception as exc:
        logger.error(
            "Schema inspection failed: %s",
            exc,
        )
        raise


if __name__ == "__main__":
    main()