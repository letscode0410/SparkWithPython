from pyspark import SparkContext


def parseData(line):
    lines = line.split("::")
    return (lines[2], float(lines[3]))


sc = SparkContext("local[2]", "numberOfConnections")
lines = sc.textFile("data/friendsdata.csv")
age_connections_rdd = lines.map(parseData)
# age_connectionsRdd = lines.map(lambda x: (x.split(",")(2), (x.split(",")[3], 1)))
connections_rdd = age_connections_rdd.mapValues(lambda x: (x, 1))
aggregated_connections = connections_rdd.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
average_connections_rdd = aggregated_connections.map(lambda x: (x[0], x[1][0]/x[1][1])).sortBy(lambda x: x[1], False)
for item in average_connections_rdd.collect():
    print(item)
