from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("age", IntegerType()),
    StructField("country", StringType()),
    StructField("player_id", IntegerType()),
    StructField("player_name", StringType()),
    StructField("role", StringType()),
    StructField("team_id", IntegerType())

])


spark_conf = SparkConf()
spark_conf.set("spark.master","local[2]")
spark_conf.set("spark.app.name", "jsonread")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

data = spark.read.format("json")\
    .schema(schema)\
    .option("path", "../data/players.json")\
    .load()

data.printSchema()
data.show()
spark.stop()