import json
import re
from pyspark import SparkContext

# A hack to avoid having to pass 'sc' around
dummyrdd = None
def setDefaultAnswer(rdd): 
	global dummyrdd
	dummyrdd = rdd

def task1(amazonInputRDD):
        return amazonInputRDD.map(lambda x: re.findall(r"user(\d+) product(\d+) (\d\.\d)", x)[0])

def task2(amazonInputRDD):
        # Assumes there are no duplicate reviews
        return task1(amazonInputRDD)\
            .map(lambda x: ("user" + x[0], float(x[2])))\
            .aggregateByKey((0,0), lambda acc,v: (acc[0]+1,acc[1]+v),\
                         lambda v1,v2: (v1[0]+v2[0],v1[1]+v2[1]))\
            .mapValues(lambda v: v[1]/v[0])

def task3(amazonInputRDD):
        return task1(amazonInputRDD) \
            .map(lambda x: ("product"+x[1], float(x[2]))) \
            .groupByKey() \
            .mapValues(lambda v: max(sorted(list(v), reverse=True), key=list(v).count))

def task4(logsRDD):
        return logsRDD.map(lambda x: (re.findall(r"\[\d\d/\w{3}/(\d{4}).+\]",x)[0], 1)) \
            .reduceByKey(lambda v1, v2: v1+v2)

def task5_flatmap(x):
        return [re.sub("[^\w ]","",x) for x in x.split()]

def task6(playRDD):
        return playRDD.filter(lambda x: len(x) > 0) \
            .map(lambda x: (x.split()[0], (x, len(x.split())))) \
            .filter(lambda x: x[1][1]>10)   

def task7_flatmap(x):
        return [e["surname"] for e in x["laureates"]]

def task8(nobelRDD):
        return nobelRDD.map(json.loads) \
            .map(lambda x: (x["category"], task7_flatmap(x))) \
            .reduceByKey(lambda v1, v2: v1+v2)

def task9(logsRDD, l):
        return logsRDD.map(lambda x: re.findall(r"(.+) - - \[(\d{2}/\w{3}/\d{4}).+\]",x)[0]) \
            .groupByKey() \
            .filter(lambda x: all([d in x[1] for d in l])) \
            .map(lambda x: x[0])

def task10(bipartiteGraphRDD):
        return bipartiteGraphRDD.aggregateByKey(0, lambda acc, v: acc+1, lambda v1, v2: v1+v2) \
            .map(lambda x: (x[1], x[0])) \
            .aggregateByKey(0, lambda acc, v: acc+1, lambda v1, v2: v1+v2)
