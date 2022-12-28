from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, monotonically_increasing_id, col, when
from pyspark.sql.types import IntegerType

if __name__ == '__main__':
    sparkConf = SparkConf()
    sparkConf.set("spark.master", "local[3]")
    sparkConf.set("spark.app.name", "UDF sample")

    spark = SparkSession.builder \
        .config(conf=sparkConf) \
        .getOrCreate()

    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981

    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
    raw_df.printSchema()
    # final_df = raw_df.withColumn("id",monotonically_increasing_id()) \
    #     .withColumn("year", expr("""
    #            case when year < 21 then year + 2000
    #            when year < 100 then year + 1900
    #            else year
    #            end
    #            """))

    # final_df = raw_df.withColumn("id", monotonically_increasing_id()) \
    #     .withColumn("year", expr("""
    #                case when year < 21 then cast(year as int) + 2000
    #                when year < 100 then cast(year as int) + 1900
    #                else year
    #                end
    #                """))

    # final_df = raw_df.withColumn("id", monotonically_increasing_id()) \
    #     .withColumn("year", expr("""
    #                   case when year < 21 then year+ 2000
    #                   when year < 100 then year + 1900
    #                   else year
    #                   end
    #                   """).cast(IntegerType()))

    final_df = raw_df.withColumn("id", monotonically_increasing_id()) \
        .withColumn("year", col("year").cast(IntegerType())) \
        .withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", expr("cast(month as int)")) \
        .withColumn("year", when(col("year") < 21, col("year") + 2000) \
                    .when(col("year") < 100, col("year") + 1900) \
                    .otherwise(col("year"))) \
        .withColumn("dob", expr("to_date(concat(day,'/',month,'/',year),'d/M/y')"))\
        .drop("year", "day", "month") \
        .dropDuplicates(["name", "dob"]) \
        .sort("dob")
    #    .sort(col("dob").desc()) works
    #    .sort(expr("dob desc"))  doesnt work


    # .withColumn("year", expr("""
    #                   case when year < 21 then year + 2000
    #                   when year < 100 then year + 1900
    #                   else year
    #                   end
    #                   """))
    final_df.show()
