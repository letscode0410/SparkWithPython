from pyspark import SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()
sparkConf.set("spark.master", "local[2]")
sparkConf.set("spark.app.name", "readcsv")

spark = SparkSession.builder\
    .config(conf=sparkConf)\
    .getOrCreate()

dataDf = spark.read\
    .format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .option("path","../data/orders.csv")\
    .load()

dataDf.printSchema()
dataDf.show()


