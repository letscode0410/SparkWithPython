from pyspark import SparkConf
from pyspark.sql import functions as f, SparkSession

# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#aggregate-functions

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

    # invoices_df.select(f.count('*').alias('count *'),
    #                    f.sum("Quantity").alias("Total Quantity"),
    #                    f.avg("UnitPrice").alias("Unit Price Average"),
    #                    f.countDistinct("StockCode").alias("Distinct Stock Codes")).show()

    # count * and count 1 gives same results whereas count field ignore the null values
    #     invoices_df.selectExpr("count(*) as `count *`", "count(1) as `count 1`",
    #                            "count(StockCode) as `count field`",
    #                            "sum(Quantity) as `Total Quantity`",
    #                            "count(distinct(StockCode)) as `Distinct Codes`").show()

    invoices_df.createOrReplaceTempView("invoices")

    # spark.sql("""select Country, InvoiceNo , sum(Quantity) as TotalQuantity ,
    #               round(sum(Quantity * UnitPrice) ,2) as `Invoice Value`
    #               from invoices
    #               group by Country, InvoiceNo""").show()

    # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.html#pyspark.sql.GroupedData

    # invoices_df.groupBy("Country", "InvoiceNo") \
    #     .agg(f.sum("Quantity").alias("TotalQuantity"),
    #          f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("Invoice"),
    #          f.expr("round(sum(Quantity * UnitPrice) ,2)").alias("Invoice")).show()

    InvoiceValue = f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")
    invoices_weekwise_df = invoices_df.withColumn("InvoiceDate", f.to_date("InvoiceDate", "dd-MM-yyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekOfYear", f.weekofyear("InvoiceDate")) \
        .groupBy("Country", "WeekOfYear") \
        .agg(f.countDistinct("InvoiceNo").alias("NumInvoices"),
             f.sum("Quantity").alias("TotalQuantity"),
             InvoiceValue)

    invoices_weekwise_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", "../../data/invoices_parquet") \
        .save()


