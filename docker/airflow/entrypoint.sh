#!/bin/bash
set -e

echo "========================================"
echo "NYC Taxi Data Platform - Airflow"
echo "========================================"

echo ""
echo "Java version:"
java -version

echo ""
echo "Spark version:"
spark-submit --version

echo ""
echo "Starting Airflow:"
echo "$@"

# Airflow commands
case "$1" in
    api-server|scheduler|dag-processor|triggerer|standalone|version|providers|connections|dags|tasks|db|users|variables)
        exec airflow "$@"
        ;;
    *)
        exec "$@"
        ;;
esac