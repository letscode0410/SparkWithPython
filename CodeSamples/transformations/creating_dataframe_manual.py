from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from pyspark.sql import SparkSession
from pyspark import SparkConf


def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(col(fld), fmt))


if __name__ == "__main__":
    my_schema = StructType([
        StructField("Id", StringType()),
        StructField("EventDate", StringType())
    ])

    my_rows = [("123", "10/04/1991"), ("124", "11/04/1991")]

    spark_conf = SparkConf()
    spark_conf.set("spark.master", "local[2]")
    spark_conf.set("spark.app.name", "manualDf")

    spark = SparkSession.builder.config(conf=spark_conf) \
        .enableHiveSupport().getOrCreate()

    base_rdd = spark.sparkContext.parallelize(my_rows)
    base_df = spark.createDataFrame(base_rdd, my_schema)
    base_df.show()

    new_df = to_date_df(base_df, "M/d/y", "EventDate")
    new_df.show()
