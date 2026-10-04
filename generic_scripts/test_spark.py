from generic_scripts.utils.sparksession import get_spark_session

spark = get_spark_session()

data = [
    ("Pranoti", 24),
    ("Surya", 25),
    ("Test", 30)
]

df = spark.createDataFrame(data, ["name", "age"])

print("===== SPARK CONNECTION TEST =====")
df.show()

print("Number of records:", df.count())

print("===== DATABASES =====")
spark.sql("SHOW DATABASES").show(truncate=False)

print("===== TABLES =====")
spark.sql("SHOW TABLES IN dataforge_landing").show(truncate=False)

print("===== READ TABLE =====")
df = spark.table("dataforge_landing.green_taxi")

df.printSchema()
df.show(5)

spark.stop()