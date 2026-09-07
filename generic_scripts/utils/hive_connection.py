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