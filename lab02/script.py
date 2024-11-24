import collections
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Lab02") \
    .getOrCreate()


rdd = spark.sparkContext.textFile("/app/u.data")
pairRDD = rdd.map(lambda x: (x.split("\t")[1], int(x.split("\t")[2])))

def printStat(inp):
    ind, marks = inp
    marks = [marks.get(i, 0) for i in range(1, 6)]
    print(f'Marks for film {ind}: 1 -> {marks[0]}, 2 -> {marks[1]}, 3 -> {marks[2]}, 4 -> {marks[3]}, 5 -> {marks[4]}')

for i in pairRDD.groupByKey().mapValues(lambda x: dict(collections.Counter(x))).collect():
    printStat(i)


allMarks = (pairRDD.map(lambda x: ("ALL", x[1]))
                   .groupByKey()
                   .mapValues(lambda x: dict(collections.Counter(x)))
                   .collect())
for x in allMarks:
    printStat(x)