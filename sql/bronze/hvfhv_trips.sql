CREATE EXTERNAL TABLE IF NOT EXISTS dataforge_landing.hvfhv_trips (
    hvfhs_license_num STRING,   
    dispatching_base_num STRING,
    originating_base_num STRING,
    request_datetime TIMESTAMP,    
    on_scene_datetime TIMESTAMP,  
    pickup_datetime TIMESTAMP,    
    dropoff_datetime TIMESTAMP,    
    PULocationID INT,        
    DOLocationID INT,        
    trip_miles DOUBLE,          
    trip_time BIGINT,           
    base_passenger_fare DOUBLE, 
    tolls DOUBLE,               
    bcf DOUBLE,                 
    sales_tax DOUBLE,           
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,         
    tips DOUBLE,                
    driver_pay DOUBLE,          
    shared_request_flag STRING, 
    shared_match_flag STRING,  
    access_a_ride_flag STRING, 
    wav_request_flag STRING,    
    wav_match_flag STRING                 
)
PARTITIONED BY (
    year STRING,
    month STRING,
    etl_batch_id STRING
)
STORED AS PARQUET
LOCATION 's3a://dataforge-lake/raw/taxi/hvfhv/';