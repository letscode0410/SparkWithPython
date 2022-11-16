from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local[2]", "pythonWordCount")
sc.setLogLevel("ERROR")
lines = sc.textFile("data/wordcount.txt")
words = lines.flatMap(lambda x: x.split())
words_count = words.map(lambda x: (x.lower()))
count_result = words_count.countByValue()

for key, value in count_result.items():
    print(key, value)
sc.stop()
