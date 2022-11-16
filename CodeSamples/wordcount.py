from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local[2]", "pythonWordCount")
sc.setLogLevel("ERROR")
lines = sc.textFile("data/wordcount.txt")
words = lines.flatMap(lambda x: x.split())
words_count = words.map(lambda x: (x.lower(), 1))
count_result = words_count.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False)  # sortByKey(False)
finalRdd = count_result.collect()

for x in finalRdd:
    print(x)
# stdin.readline()
sc.stop()
