#!/usr/bin/python

from pyspark import SparkContext, SparkConf

# To submit this python application on Spark cluster for execution:
# $SPARK_HOME/bin/spark-submit <path_to_application> --master spark://<master_public_dns:7077

# The helper value sc is created in spark-shell, not automatically
# created in spark-submit: must instantiate own SparkContext to use
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

