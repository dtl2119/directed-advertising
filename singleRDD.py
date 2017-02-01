#!/usr/bin/python

from pyspark import SparkContext, SparkConf

# To submit this python application on Spark cluster for execution:
# $SPARK_HOME/bin/spark-submit <path_to_application> --master spark://<master_public_dns:7077

# sc is a helper value created in the spark-shell, 
# but not automatically created with spark-submit. 
# You must instantiate your own SparkContext and use that
conf = SparkConf().setAppName("adTarget")
sc = SparkContext(conf=conf)

# For files in hdfs
#file = sc.textFile("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/test.txt")

# For files on disk (file must be on all nodes in the cluster)
webLogRdd = sc.textFile("file:///home/ubuntu/directed-advertising/small_batch_file.txt/")

# Parse csv: time, user_id, product_id, category_id, action (search or buy)
parsedRdd = webLogRdd.map(lambda line: line.split(", "))


# Aggregate (user_id, category) keys of users who made the purchase (filter out later)
# Don't advertise to users who searched in a category and made the purchase
buyKeys = parsedRdd.filter(lambda line: "buy" in line).map(lambda n: (n[1], n[3])).collect()

# Group RDDs --> (user_id, category_id) = [list, of, searches]
groupedRdd = parsedRdd.map(lambda n: ((n[1], n[3]), n[2])).groupByKey().map(lambda x: [(x[0]), list(x[1])])

# Filter out users who made the purchase in the category they were searching in
resultRdd = groupedRdd.filter(lambda x: x[0] not in buyKeys).map(lambda x: [x[0], x[1]])

# Dictionary result
result = dict(resultRdd.collect())

# FIXME
for k, v in result.iteritems():
    print "%s: %s" % (k, v)
####

#webLogRdd = sc.textFile("file:///home/ubuntu/directed-advertising/small_batch_file.txt/")
#parsedRdd = webLogRdd.map(lambda line: line.split(", "))
#
#
#buyKeys = parsedRdd.filter(lambda line: "buy" in line).map(lambda n: (n[1], n[3])).collect()
#
#groupedRdd = parsedRdd.map(lambda n: ((n[1], n[3]), n[2])).groupByKey().map(lambda x: [(x[0]), list(x[1])])
#resultRDD = groupedRdd.filter(lambda x: x[0] not in buyKeys).map(lambda x: [x[0], x[1]])
#
#
#result = dict(resultRDD.collect())
#
