"""------------------- Hive SQL Executor -------------------
Reads and executes one SQL file or every SQL file in a directory against HiveServer2.
Usage: python hive_sql_executor.py <sql_file_or_directory> [--host HOST] [--port PORT]
Example: python hive_sql_executor.py sql/bronze --host hiveserver2 --port 10000
This script validates SQL paths, splits statements safely, and runs them through a HiveServer2 connection.
------------------- ------------------- ----------------------"""

import argparse
import logging
from pathlib import Path

from pyhive import hive


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger("pyhive.hive").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def read_sql_file(sql_file_path: Path) -> str:
    """
    Read SQL statements from a SQL file.
    """

    if not sql_file_path.exists():
        raise FileNotFoundError(
            f"SQL file not found: {sql_file_path}"
        )

    logger.info(
        "Reading SQL file: %s",
        sql_file_path,
    )

    sql = sql_file_path.read_text(
        encoding="utf-8"
    ).strip().rstrip(";").rstrip()

    if not sql:
        raise ValueError(
            f"SQL file is empty: {sql_file_path}"
        )

    return sql


def split_sql_statements(sql: str) -> list[str]:
    """Split SQL on semicolons outside quoted strings and comments."""

    statements = []
    statement_start = 0
    index = 0
    quote = None
    in_line_comment = False
    in_block_comment = False

    while index < len(sql):
        character = sql[index]
        next_character = sql[index + 1] if index + 1 < len(sql) else ""

        if in_line_comment:
            if character in "\r\n":
                in_line_comment = False
            index += 1
            continue

        if in_block_comment:
            if character == "*" and next_character == "/":
                in_block_comment = False
                index += 2
            else:
                index += 1
            continue

        if quote:
            if character == quote:
                if next_character == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue

        if character in "'\"`":
            quote = character
            index += 1
            continue

        if character == "-" and next_character == "-":
            in_line_comment = True
            index += 2
            continue

        if character == "/" and next_character == "*":
            in_block_comment = True
            index += 2
            continue

        if character == ";":
            statement = sql[statement_start:index].strip()
            if statement:
                statements.append(statement)
            statement_start = index + 1

        index += 1

    final_statement = sql[statement_start:].strip()
    if final_statement:
        statements.append(final_statement)

    if not statements:
        raise ValueError("SQL file contains no statements")

    return statements


def execute_sql(
    sql: str,
    host: str,
    port: int,
) -> None:
    """
    Execute SQL against HiveServer2.
    """

    logger.info(
        "Connecting to HiveServer2 at %s:%s",
        host,
        port,
    )

    connection = hive.Connection(
        host=host,
        port=port,
    )

    cursor = connection.cursor()

    try:
        statements = split_sql_statements(sql)
        logger.info("Executing %s SQL statement(s)", len(statements))

        for statement_number, statement in enumerate(statements, start=1):
            logger.info("Executing SQL statement %s/%s", statement_number, len(statements))
            cursor.execute(statement)

        logger.info("SQL executed successfully")

    finally:
        cursor.close()
        connection.close()

        logger.info(
            "HiveServer2 connection closed"
        )


def find_sql_files(sql_path: Path) -> list[Path]:
    """Return SQL files from a file path or recursively from a directory."""

    if not sql_path.exists():
        raise FileNotFoundError(
            f"SQL path not found: {sql_path}"
        )

    if sql_path.is_file():
        return [sql_path]

    sql_files = sorted(
        path
        for path in sql_path.rglob("*.sql")
        if path.is_file()
    )

    if not sql_files:
        raise FileNotFoundError(
            f"No SQL files found under: {sql_path}"
        )

    return sql_files


def main() -> None:

    parser = argparse.ArgumentParser(
        description="Execute a SQL file or all SQL files under a directory against HiveServer2"
    )

    parser.add_argument(
        "sql_path",
        type=Path,
        help="Path to a SQL file or directory containing SQL files",
    )

    parser.add_argument(
        "--host",
        default="hiveserver2",
        help="HiveServer2 hostname",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=10000,
        help="HiveServer2 port",
    )

    args = parser.parse_args()

    for sql_file in find_sql_files(args.sql_path):
        sql = read_sql_file(sql_file)
        execute_sql(
            sql=sql,
            host=args.host,
            port=args.port,
        )


if __name__ == "__main__":
    main()