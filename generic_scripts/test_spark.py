from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AirflowSparkConnectionTest") \
    .getOrCreate()

data = [
    ("ABC", 24),
    ("PQR", 25),
    ("XYZ", 30)
]

df = spark.createDataFrame(data, ["name", "age"])

print("===== SPARK CONNECTION TEST =====")
df.show()

print("Number of records:", df.count())

spark.stop()