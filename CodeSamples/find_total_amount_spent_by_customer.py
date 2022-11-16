from pyspark import SparkContext


def parseData(line):
    lines = line.split(",")
    return (lines[0], float(lines[2]))


sc = SparkContext("local[2]", "customerDataAnalysis")
customers_data = sc.textFile("data/customerorders.csv")
customers_orderPrice_rdd = customers_data.map(parseData)
customer_totalSpent = customers_orderPrice_rdd.reduceByKey(lambda x, y: x + y)
customer_totalSpent_sorted = customer_totalSpent.sortBy(lambda x: x[1], False)

for record in customer_totalSpent_sorted.collect():
    print(record)

