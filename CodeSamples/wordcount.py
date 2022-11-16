from pyspark import SparkContext

sc = SparkContext("local[2]", "pythonWordCount")

lines = sc.textFile("data/wordcount.txt")
words = lines.flatMap(lambda x: x.split())
words_count = words.map(lambda x: (x, 1))
count_result = words_count.reduceByKey(lambda x, y: x + y)
finalRdd = count_result.collect()

for x in finalRdd:
    print(x)
