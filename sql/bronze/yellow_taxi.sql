CREATE EXTERNAL TABLE IF NOT EXISTS dataforge_landing.yellow_taxi (
    vendor_id BIGINT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    ratecode_id DOUBLE,
    store_and_fwd_flag STRING,
    pu_location_id BIGINT,
    do_location_id BIGINT,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE
)
PARTITIONED BY (
    year STRING,
    month STRING,
    etl_batch_id STRING
)
STORED AS PARQUET
LOCATION 's3a://dataforge-lake/raw/taxi/yellow/';
