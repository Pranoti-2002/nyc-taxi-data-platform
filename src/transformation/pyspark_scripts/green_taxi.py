# batch id : 20261020060351_trip_demand_dataforge
from generic_scripts.utils.sparksession import get_spark_session

#spark = get_spark_session()
# df = spark.table("dataforge_landing.green_taxi")
# df.printSchema()
# df.show(5, truncate=False)
# df.count()

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("InspectYellowTaxiVendor")
    .enableHiveSupport()
    .getOrCreate()
)

path = (
    "s3a://dataforge-lake/raw/taxi/yellow/"
    "year=2020/month=01/"
    "etl_batch_id=20261020060351_trip_demand_dataforge/"
    "yellow_tripdata_2020-01.parquet"
)

df = spark.read.parquet(path)

df.printSchema()
df.select("VendorID").show(10, truncate=False)