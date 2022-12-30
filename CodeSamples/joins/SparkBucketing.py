from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sparkConf = SparkConf()
    sparkConf.set("spark.app.name", "Spark Bucketing")
    sparkConf.set("spark.master", "local[3]")

    spark = SparkSession.builder \
        .config(conf=sparkConf) \
        .enableHiveSupport() \
        .getOrCreate()

    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    spark.sql("create database if not exists flight_db")
    spark.sql("use flight_db")

    flight_time_df1.coalesce(1).write.bucketBy(3, "id").mode("overwrite").saveAsTable("flight_db.flight_data1")
    flight_time_df2.coalesce(1).write.bucketBy(3, "id").mode("overwrite").saveAsTable("flight_db.flight_data2")

