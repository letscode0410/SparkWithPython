from pyspark import SparkConf
from pyspark.sql import functions as f, SparkSession

if __name__ == "__main__":
    my_rows = [("123", "10/04/1991"), ("124", "11/04/1991")]

    spark_conf = SparkConf()
    spark_conf.set("spark.master", "local[2]")
    spark_conf.set("spark.app.name", "manualDf")

    spark = SparkSession.builder.config(conf=spark_conf) \
        .getOrCreate()

    invoices_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("path", "../../data/invoices.csv") \
        .load()

    invoices_df.select(f.count('*').alias('count *'),\
                       f.sum("Quantity").alias("Total Quantity"), \
                       f.avg("UnitPrice").alias("Unit Price Average"),\
                       f.countDistinct("StockCode").alias("Distinct Stock Codes")).show()
