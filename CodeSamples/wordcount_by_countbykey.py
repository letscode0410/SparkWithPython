from pyspark import SparkContext
from sys import stdin

sc = SparkContext("local[2]", "pythonWordCount")
sc.setLogLevel("ERROR")
lines = sc.textFile("data/wordcount.txt")
words = lines.flatMap(lambda x: x.split())
words_count = words.map(lambda x: (x.lower(),1))
count_result = words_count.countByKey()

for key, value in count_result.items():
    print(key, value)
sc.stop()
