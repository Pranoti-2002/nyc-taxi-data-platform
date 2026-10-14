from pyspark.sql import SparkSession

def get_spark_session():
    spark = (  
               SparkSession.builder
               .appName("Dataforge-platform")
               .config("spark.hadoop.hive.metastore.uris", "thrift://hive:9083")
               .config("spark.ui.showConsoleProgress", "false")
               .enableHiveSupport()
               .getOrCreate()
        )
    spark.sparkContext.setLogLevel("WARN")
    return spark 
