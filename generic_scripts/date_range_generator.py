import logging
from pathlib import Path
import sys
import pendulum


MAX_END_MONTH = pendulum.datetime(2025, 12, 1, tz="UTC")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def read_processing_config(config_file: Path) -> tuple[str, int]:
    if not config_file.exists():
        raise FileNotFoundError(
            f"Processing configuration file not found: {config_file}"
        )

    content = config_file.read_text(
        encoding="utf-8"
    ).strip()

    if not content:
        raise ValueError(
            f"Processing configuration file is empty: {config_file}"
        )

    try:
        start_month, process_months = content.split("|")
    except ValueError as exc:
        raise ValueError(
            "Expected format: YYYY-MM|number_of_months"
        ) from exc

    start_month = start_month.strip()
    process_months = process_months.strip()

    try:
        start_date = pendulum.from_format(start_month,"YYYY-MM",tz="UTC",)
    except ValueError as exc:
        raise ValueError(
            f"Invalid start month: {start_month}. "
            "Expected format: YYYY-MM"
        ) from exc

    try:
        process_months = int(process_months)
    except ValueError as exc:
        raise ValueError(
            f"Invalid process_months value: {process_months}"
        ) from exc

    if process_months < 1:
        raise ValueError(
            "process_months must be greater than or equal to 1"
        )

    return start_date.strftime("%Y-%m"), process_months


def calculate_end_month(
    start_month: str,
    process_months: int,
) -> str:
    start_date = pendulum.from_format(
        start_month,
        "YYYY-MM",
        tz="UTC",
    )

    end_date = start_date.add(months=process_months - 1)

    if end_date > MAX_END_MONTH:
        raise ValueError(
            f"Calculated end month {end_date.strftime('%Y-%m')} "
            f"exceeds maximum allowed month "
            f"{MAX_END_MONTH.strftime('%Y-%m')}"
        )

    return end_date.strftime("%Y-%m")


def write_processing_range(output_file: Path,start_month: str,end_month: str,) -> None:
    output_file.parent.mkdir(parents=True,exist_ok=True,)

    output_file.write_text(f"{start_month}|{end_month}\n",encoding="utf-8",)

    logger.info(
        "Processing range written: %s → %s",start_month,end_month,
    )


def main() -> None:
    config_file = Path(f"/opt/project/parfiles/processing_range.txt")

    output_file = Path(f"/opt/project/parfiles/processing_range_output.txt")

    start_month, process_months = read_processing_config(config_file)

    end_month = calculate_end_month(start_month,process_months,)

    write_processing_range(output_file,start_month,end_month,)


if __name__ == "__main__":
    main()