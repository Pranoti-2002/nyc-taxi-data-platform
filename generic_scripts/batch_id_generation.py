# generic_scripts/batch_id_generation.py
import datetime
import logging
import sys
from typing import Optional

from generic_scripts.utils.hive_connection import get_hive_connection

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def check_incomplete_batch_id(cursor, source_system: str, phase_name: str) -> Optional[str]:
    """
    Checks if any incomplete batch_id is present for the given source and phase.
    Returns the existing incomplete batch_id if found, otherwise None.
    """
    logger.info(f"Checking for incomplete etl_batch_id for source_system={source_system}, phase_name={phase_name}")
    
    query = "SELECT * FROM DATAFORGE_AUDIT.BATCH_LOG WHERE source_system = %s AND phase_name = %s"
    
    try:
        cursor.execute(query, (source_system, phase_name))
        rows = cursor.fetchall()

        for row in rows:
            etl_batch_id = row[0]
            batch_status = row[3]

            if batch_status != "C":
                logger.warning(
                    "Found incomplete etl_batch_id=%s for source_system=%s (status=%s)",
                    etl_batch_id,
                    source_system,
                    batch_status,
                )
                return etl_batch_id

        logger.info(
            "No incomplete etl_batch_id found for source_system=%s and phase_name=%s",
            source_system,
            phase_name,
        )
        return None
    except Exception as e:
        logger.exception(
            "Failed while checking incomplete etl_batch_id for source_system=%s, phase_name=%s",
            source_system,
            phase_name,
        )
        return None


def generate_etl_batch_id(cursor, source_system: str, phase_name: str) -> str:
    """
    Generates a fresh etl_batch_id, inserts it into the audit table, and saves it to a local file.
    """
    logger.info(
        f"Generating fresh etl_batch_id for source_system={source_system}, phase_name={phase_name}"
    )

    try:
        current_time = datetime.datetime.now()
        etl_batch_id = (
            current_time.strftime("%Y%m%d%H%M%S")
            + f"_{source_system}"
        )

        batch_status = "S"
        record_count_processed = 0
        record_count_failed = 0
        error_message = ""

        insert_query = """
        INSERT INTO dataforge_audit.batch_log 
        (
            etl_batch_id,
            phase_name,
            source_system,
            batch_status,
            record_count_processed,
            record_count_failed,
            error_message,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            etl_batch_id,
            phase_name,
            source_system,
            batch_status,
            record_count_processed,
            record_count_failed,
            error_message,
            current_time,
            current_time
        )

        cursor.execute(insert_query, values)

        logger.info(f"Generated etl_batch_id: {etl_batch_id}")
        
        # Write to local file
        file_path = f"/opt/project/parfiles/{source_system}/etl_batch_id.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(etl_batch_id)
        
        logger.info(f"etl_batch_id written to {file_path}")
        
        return etl_batch_id

    except Exception as e:
        logger.exception(f"Failed inserting into batch_log: {e}")
        raise
    

def main():
    if len(sys.argv) != 3:
        print("Usage: python batch_id_generation <source_system> <phase_name>")
        sys.exit(1)
    
    source_name = sys.argv[1]    
    phase_name = sys.argv[2]
    conn = None
    cursor = None

    try:
        # Connect to database
        conn = get_hive_connection()
        if not conn:
            logger.error("Can't connect to Hive. Please check DB availability.")
            sys.exit(1)
        
        cursor = conn.cursor()
        logger.info("Connected successfully to Hive")

        # Check for existing incomplete batch
        inc_batch_id = check_incomplete_batch_id(cursor, source_name, phase_name)
        if inc_batch_id:
            logger.error(f"Aborting: Incomplete batch_id found: {inc_batch_id}")
            sys.exit(1)

        # Generate new batch ID
        generate_etl_batch_id(cursor, source_name, phase_name)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        # Ensure resources are closed
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()    