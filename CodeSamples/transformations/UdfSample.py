from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType
import re


def format_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    sparkConf = SparkConf()
    sparkConf.set("spark.master", "local[3]")
    sparkConf.set("spark.app.name", "UDF sample")

    spark = SparkSession.builder \
        .config(conf=sparkConf) \
        .getOrCreate()

    survey_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("path", "../data/survey.csv") \
        .load()

    survey_df.show(10)
    format_gender_udf = udf(format_gender, returnType=StringType())
    print("Catalog Entry : ")
    [print(func) for func in spark.catalog.listFunctions() if 'format_gender_udf' == func.name]
    formatted_survey_df = survey_df.withColumn("Gender", format_gender_udf('Gender'))
    formatted_survey_df.show()

    spark.udf.register("format_gender_udf", format_gender, returnType=StringType())
    print("Catalog Entry : ")
    [print(func) for func in spark.catalog.listFunctions() if 'format_gender_udf' == func.name]

    formatted_survey_df1 = survey_df.withColumn("Gender", expr('format_gender_udf(Gender)'))
    formatted_survey_df1.show()

