CREATE EXTERNAL TABLE IF NOT EXISTS dataforge_landing.fhv_trips (
    dispatching_base_num STRING,
    pickup_datetime TIMESTAMP,        
    dropOff_datetime TIMESTAMP,         
    PUlocationID BIGINT,        
    DOlocationID BIGINT,               
    SR_Flag BIGINT,               
    Affiliated_base_number STRING             
)
PARTITIONED BY (
    year STRING,
    month STRING,
    etl_batch_id STRING
)
STORED AS PARQUET
LOCATION 's3a://dataforge-lake/raw/taxi/fhv/';