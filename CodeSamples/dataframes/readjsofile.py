from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf = SparkConf()
spark_conf.set("spark.master","local[2]")
spark_conf.set("spark.app.name", "jsonread")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

data = spark.read.format("json")\
    .option("inferSchema",True)\
    .option("path", "../data/players.json")\
    .load()

data.printSchema()

spark.stop()