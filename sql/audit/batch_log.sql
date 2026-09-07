CREATE EXTERNAL TABLE audit.batch_log (
    etl_batch_id STRING,
    phase_name STRING,
    source_system STRING,
    batch_status STRING,
    record_count_processed INT,
    record_count_failed INT,
    error_message STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3a://dataforge-lake/audit/batch_log/';