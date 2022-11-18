from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf = SparkConf()
spark_conf.set("spark.master","local[2]")
spark_conf.set("spark.app.name", "jsonread")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# BY DEFAULT OPTION IS PERMISSIVE. AND FAIL PAST WILL THROW ERROR AND DROPMALFORMED WILL JUST DROP THE RECORD
data = spark.read.format("json")\
    .option("inferSchema",True)\
    .option("mode","DROPMALFORMED")\
    .option("path", "../data/players-notformatted.json")\
    .load()

data.printSchema()

data.show(truncate=False)

spark.stop()