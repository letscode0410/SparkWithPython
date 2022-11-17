from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("order_date", TimestampType()),
    StructField("order_customer_id", IntegerType()),
    StructField("order_status", StringType())
])

sparkConf = SparkConf()
sparkConf.set("spark.master", "local[2]")
sparkConf.set("spark.app.name", "readcsv")

spark = SparkSession.builder \
    .config(conf=sparkConf) \
    .getOrCreate()

dataDf = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("path", "../data/orders.csv") \
    .schema(schema) \
    .load()

dataDf.printSchema()
dataDf.show()
