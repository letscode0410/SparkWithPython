from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark_conf = SparkConf()
    spark_conf.set("spark.master", "local[2]")
    spark_conf.set("spark.app.name", "parquetRead")

    spark = SparkSession.builder.config(conf=spark_conf)\
        .enableHiveSupport().getOrCreate()
    data = spark.read \
        .option("path", "../data/users.parquet") \
        .load()
    spark.sql("create database if not exists testDb")
    spark.catalog.setCurrentDatabase("testDb")

    data.write.format("json").mode("overwrite").saveAsTable("sampletable")
