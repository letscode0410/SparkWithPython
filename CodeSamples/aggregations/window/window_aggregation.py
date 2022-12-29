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

    window = Window.partitionBy("Country").orderBy("WeekOfYear").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    windowFor3weeks = Window.partitionBy("Country").orderBy("WeekOfYear").rowsBetween(-2,
                                                                             Window.currentRow)
    invoices_df.withColumn("RunningTotal", functions.sum("TotalQuantity").over(window)).show()

    invoices_df.withColumn("RunningTotal", functions.sum("TotalQuantity").over(windowFor3weeks)).show()

