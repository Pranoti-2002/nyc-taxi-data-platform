CREATE EXTERNAL TABLE IF NOT EXISTS dataforge_landing.green_taxi(
    VendorID INT, 
    lpep_pickup_datetime TIMESTAMP,          
    lpep_dropoff_datetime TIMESTAMP,           
    store_and_fwd_flag STRING,              
    RatecodeID  BIGINT,                      
    PULocationID  INT,                     
    DOLocationID  INT,                      
    passenger_count BIGINT,                      
    trip_distance DOUBLE,                                          
    fare_amount DOUBLE,                                            
    extra DOUBLE,                                                  
    mta_tax DOUBLE,                                              
    tip_amount DOUBLE,                                             
    tolls_amount DOUBLE,                                          
    ehail_fee  DOUBLE,                                             
    improvement_surcharge DOUBLE,                                  
    total_amount DOUBLE,                                        
    payment_type BIGINT,                                           
    trip_type BIGINT,                                                
    congestion_surcharge DOUBLE
)
PARTITIONED BY (
    year STRING,
    month STRING,
    etl_batch_id STRING
)
STORED AS PARQUET
LOCATION "s3a://dataforge-lake/raw/taxi/green/";                    
