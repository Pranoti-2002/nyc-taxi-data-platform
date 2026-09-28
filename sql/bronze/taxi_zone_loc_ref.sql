CREATE EXTERNAL TABLE IF NOT EXISTS dataforge_lookup.taxi_zone_loc_ref (
    LocationID INT,
    Borough STRING,
    Zone STRING,
    service_zone STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION "s3a://dataforge-lake/raw/lookup/taxi_zone_loc_ref";