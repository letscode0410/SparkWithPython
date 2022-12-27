from pyspark.sql.functions import to_date, col, regexp_extract, substring_index
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from pyspark.sql import SparkSession
from pyspark import SparkConf

if __name__ == "__main__":
    spark_conf = SparkConf()
    spark_conf.set("spark.master", "local[2]")
    spark_conf.set("spark.app.name", "logfile_regex")

    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    logs = spark.read.text("../data/apache_logs.txt")
    logs.printSchema()

    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    logs_df = logs.select(regexp_extract('value', log_reg, 1).alias('ip'),
                          regexp_extract('value', log_reg, 4).alias('date'),
                          regexp_extract('value', log_reg, 6).alias('request'),
                          regexp_extract('value', log_reg, 10).alias('referrer'))
    logs_df.where("trim(referrer) != '-'") \
        .withColumn("referrer", substring_index("referrer", "/", 3)).groupby("referrer").count().sort("count",
                                                                                                      ascending=False).show(
        100, truncate=False)
