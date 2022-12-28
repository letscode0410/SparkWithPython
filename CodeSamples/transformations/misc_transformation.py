from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, monotonically_increasing_id
from pyspark.sql.types import  IntegerType

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

    raw_df = spark.createDataFrame(data_list).toDF("name", "age", "month", "year").repartition(3)
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

    final_df = raw_df.withColumn("id", monotonically_increasing_id()) \
        .withColumn("year", expr("""
                      case when year < 21 then year+ 2000
                      when year < 100 then year + 1900
                      else year
                      end
                      """).cast(IntegerType()))
    final_df.show()
