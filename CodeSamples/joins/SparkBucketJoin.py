from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sparkConf = SparkConf()
    sparkConf.set("spark.app.name", "Spark Bucketing")
    sparkConf.set("spark.master", "local[3]")

    spark = SparkSession.builder \
        .config(conf=sparkConf) \
        .enableHiveSupport()\
        .getOrCreate()
    '''
    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    spark.sql("create database if not exists flight_db")
    spark.sql("use flight_db")

    flight_time_df1.coalesce(1).write.bucketBy(3, "id").mode("overwrite").saveAsTable("flight_db.flight_data1")
    flight_time_df2.coalesce(1).write.bucketBy(3, "id").mode("overwrite").saveAsTable("flight_db.flight_data2") try with one is 3 buckets and one is 2 bucket the there will be one exchange 
    '''
    flight_time_df1 = spark.read.table("flight_db.flight_data1")
    flight_time_df2 = spark.read.table("flight_db.flight_data2")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

    join_expr = flight_time_df1.id == flight_time_df2.id

    joined_data = flight_time_df1.join(flight_time_df2, join_expr, "inner")

    joined_data.collect()

    input("press any to exit")


