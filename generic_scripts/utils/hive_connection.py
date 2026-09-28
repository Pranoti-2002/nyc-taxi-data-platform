"""------------------- Hive Connection Helper -------------------
Creates a shared PyHive connection to the Hive metastore for audit, landing, and transformation jobs using environment variables for host, port, user, and database.
Usage: from generic_scripts.utils.hive_connection import get_hive_connection
example: conn = get_hive_connection()
The helper defaults to hiveserver2:10000 and the default database if the environment is not configured.
------------------- ------------------- ----------------------"""

import os

try:
    from pyhive import hive
except ImportError:  # pragma: no cover - optional dependency for local/test environments
    hive = None


def get_hive_connection():
    if hive is None:
        raise ModuleNotFoundError("pyhive is required for Hive connectivity")

    return hive.Connection(
        host=os.getenv("HIVE_SERVER_HOST", "hiveserver2"),
        port=int(os.getenv("HIVE_SERVER_PORT", "10000")),
        username=os.getenv("HIVE_USER", "hive"),
        database=os.getenv("HIVE_DATABASE", "default"),
        auth="NONE",
    )