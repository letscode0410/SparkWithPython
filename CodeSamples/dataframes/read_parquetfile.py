from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf = SparkConf()
spark_conf.set("spark.master","local[2]")
spark_conf.set("spark.app.name", "parquetRead")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

data = spark.read\
    .option("path", "../data/users.parquet")\
    .load()

data.printSchema()
data.show()
spark.stop()