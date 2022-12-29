from pyspark import SparkConf
from pyspark.sql import SparkSession, functions
from pyspark.sql.window import Window

if __name__ == "__main__":
    spark_conf = SparkConf()
    spark_conf.set("spark.master", "local[2]")
    spark_conf.set("spark.app.name", "manualDf")

    spark = SparkSession.builder.config(conf=spark_conf) \
        .getOrCreate()

    invoices_df = spark.read \
        .format("parquet") \
        .option("path", "../../data/invoices.parquet") \
        .load()

    window = Window.partitionBy("Country").orderBy(functions.col("InvoiceValue").desc()).rowsBetween(Window.unboundedPreceding,Window.currentRow)

    invoices_df.withColumn("Rank",functions.dense_rank().over(window)) \
        .where("Rank == 1").sort("Country","WeekOfYear").show()
