from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark_conf = SparkConf()
    spark_conf.set("spark.master", "local[2]")
    spark_conf.set("spark.app.name", "parquetRead")

    spark = SparkSession.builder.config(conf=spark_conf) \
        .enableHiveSupport().getOrCreate()
    data = spark.read \
        .option("path", "../data/flight-time.parquet") \
        .load()

    spark.sql("create database if not exists testDb")
    spark.catalog.setCurrentDatabase("testDb")

    data.write.mode("overwrite") \
        .format("csv") \
        .bucketBy(5,"OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN").saveAsTable("sampletable")
