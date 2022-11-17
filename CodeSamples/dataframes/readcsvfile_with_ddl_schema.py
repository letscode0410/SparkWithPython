from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

order_ddl = "order_id Integer,order_date Timestamp,order_customer_id Integer,order_status String"


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
    .schema(order_ddl) \
    .load()

dataDf.printSchema()
dataDf.show()
