CREATE EXTERNAL TABLE IF NOT EXISTS dataforge_lookup.taxi_zone_shapefile (
    OBJECTID INT,                           
    Shape_Leng DOUBLE,                         
    Shape_Area DOUBLE,                         
    zone STRING,                               
    LocationID INT,                         
    borough STRING,                            
    geometry BINARY                                             
)
STORED AS PARQUET
LOCATION 's3a://dataforge-lake/raw/lookup/taxi_zone_shapefile/';