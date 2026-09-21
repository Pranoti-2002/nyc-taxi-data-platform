# generic_scripts/sanitised_anr.py

import logging
import sys
from pathlib import Path

from generic_scripts.utils.hive_connection import get_connection

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def read_etl_batch_id(source_system):
    """Read ETL batch ID generated during batch initialization."""
    file_path = Path("/opt/project") / "configs" / f"{source_system}_batch_id.txt"
    logger.info("[BATCH] Reading batch metadata | path=%s", file_path)

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            etl_batch_id = file.readline().strip()

        if not etl_batch_id:
            raise ValueError("ETL batch id is empty")

        logger.info("[BATCH] Loaded batch_id=%s", etl_batch_id)
        return etl_batch_id

    except Exception:
        logger.exception("[BATCH] Failed reading batch id")
        raise


def get_table_count(cursor, schema_name, table_name, etl_batch_id):
    """Fetch record count for a table for current ETL batch."""
    query = f"""
        SELECT COUNT(*)
        FROM {schema_name}.{table_name}
        WHERE etl_batch_id = %s;
    """

    try:
        cursor.execute(query, (etl_batch_id,))
        count = cursor.fetchone()[0]
        logger.info("[COUNT] %s.%s = %s", schema_name, table_name, count)
        return count

    except Exception:
        logger.exception(
            "[COUNT] Failed fetching count | table=%s.%s", schema_name, table_name
        )
        raise


def insert_recon_log(
    cursor,
    conn,
    source_system,
    group_name,
    source_table,
    target_table,
    landing_count,
    sanitised_count,
    phase_name="sanitised",
):
    """Insert reconciliation result into audit.recon_log."""
    etl_batch_id = read_etl_batch_id(source_system)

    if landing_count == sanitised_count:
        recon_status = "C"
        failed_count = 0
    else:
        recon_status = "E"
        failed_count = abs(landing_count - sanitised_count)

    query = """
        INSERT INTO audit.recon_log
        (
            etl_batch_id,
            phase_name,
            source_system,
            recon_status,
            record_count_processed,
            record_count_failed,
            source_table,
            target_table,
            group_name,
            created_at,
            updated_at
        )
        VALUES
        (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        );
    """

    cursor.execute(
        query,
        (
            etl_batch_id,
            phase_name,
            source_system,
            recon_status,
            sanitised_count,
            failed_count,
            source_table,
            target_table,
            group_name,
        ),
    )
    conn.commit()

    logger.info(
        "[RECON] Inserted | batch_id=%s | group=%s | table=%s | status=%s",
        etl_batch_id,
        group_name,
        source_table,
        recon_status,
    )
    return recon_status


def main():
    """
    Usage:
        python sanitised_anr.py \
            <landing_schema> <landing_table> \
            <sanitised_schema> <sanitised_table> \
            <source_system> <group_name>
    """
    if len(sys.argv) != 7:
        print(
            "Usage: python sanitised_anr.py <landing_schema> <landing_table> "
            "<sanitised_schema> <sanitised_table> <source_system> <group_name>"
        )
        sys.exit(1)

    landing_schema = sys.argv[1]
    landing_table = sys.argv[2]
    sanitised_schema = sys.argv[3]
    sanitised_table = sys.argv[4]
    source_system = sys.argv[5]
    group_name = sys.argv[6]

    conn = None
    cursor = None

    try:
        conn = get_connection()
        if not conn:
            raise ConnectionError("Unable to connect to Postgres")

        cursor = conn.cursor()
        logger.info("Connected successfully to Postgres")

        etl_batch_id = read_etl_batch_id(source_system)

        landing_count = get_table_count(
            cursor, landing_schema, landing_table, etl_batch_id
        )
        sanitised_count = get_table_count(
            cursor, sanitised_schema, sanitised_table, etl_batch_id
        )

        logger.info(
            "[ANR] Comparing counts | group=%s | landing=%s | sanitised=%s",
            group_name,
            landing_count,
            sanitised_count,
        )

        status = insert_recon_log(
            cursor,
            conn,
            source_system,
            group_name,
            f"{landing_schema}.{landing_table}",
            f"{sanitised_schema}.{sanitised_table}",
            landing_count,
            sanitised_count,
        )

        if status == "E":
            logger.error(
                "[ANR] Reconciliation failed | group=%s | table=%s",
                group_name,
                landing_table,
            )
            sys.exit(1)

        logger.info(
            "[ANR] Reconciliation successful | group=%s | table=%s",
            group_name,
            landing_table,
        )

    except Exception:
        logger.exception("[ANR] Fatal error during sanitised ANR")
        sys.exit(1)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("Closed Postgres connection")


if __name__ == "__main__":
    main()