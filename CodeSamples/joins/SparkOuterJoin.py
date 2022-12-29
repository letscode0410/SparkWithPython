from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":
    sparkConf = SparkConf()
    sparkConf.set("spark.app.name", "Spark Outer Join")
    sparkConf.set("spark.master", "local[3]")

    spark = SparkSession.builder \
        .config(conf=sparkConf) \
        .getOrCreate()

    orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

    product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]

    orders = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    products = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty").withColumnRenamed("qty","prod_qty")
    join_condition = orders.prod_id == products.prod_id

    combined_Data = orders.join(products, join_condition, "outer")

    combined_Data.select("*").show()

    combined_Data\
    .drop(products.prod_id)\
    .select("order_id","prod_id", "qty","prod_name").show()


