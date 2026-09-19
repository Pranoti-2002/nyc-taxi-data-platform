"""------------------- Hive Connection Helper -------------------
Creates a PyHive connection to the configured HiveServer2 instance using environment variables for the host, port, user, and database.
Usage: imported by ETL scripts; e.g. get_hive_connection()
Example: connection = get_hive_connection()
This module centralizes the Hive connection settings shared by batch and SQL execution workflows.
------------------- ------------------- ----------------------"""

import os

from pyhive import hive


def get_hive_connection():
    return hive.Connection(
        host=os.getenv("HIVE_SERVER_HOST", "hiveserver2"),
        port=int(os.getenv("HIVE_SERVER_PORT", "10000")),
        username=os.getenv("HIVE_USER", "hive"),
        database=os.getenv("HIVE_DATABASE", "default"),
        auth="NONE",
    )