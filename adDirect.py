#!/usr/bin/python

import aws_creds
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
searchRDD = sc.textFile("file:///home/ubuntu/directed-advertising/short_search_batch.txt/")
purchaseRDD = sc.textFile("file:///home/ubuntu/directed-advertising/short_purchase_batch.txt/")

# Function to parse lines of searches and purchases (CSVs)
# Timestamp, user_id (int), product_id (int), category_id (int)
def parseLines(line):
    time, userid, product, category = line.split(", ")
    return (int(userid),int(category),int(product))


# Parse csv lines in search/purchase RDD: time, user_id, product_id, category_id
searchRDDParsed = searchRDD.map(lambda line: line.split(", "))
purchaseRDDParsed = purchaseRDD.map(lambda line: line.split(", "))

# Group RDDs --> (user_id, category_id) = [list, of, searches]
searchGroupRDD = searchRDDParsed.map(lambda n: ((n[1], n[3]), n[2])).groupByKey().map(lambda x: [(x[0]), list(x[1])])
purchaseGroupRDD = purchaseRDDParsed.map(lambda n: ((n[1], n[3]), n[2])).groupByKey().map(lambda x: [(x[0]), list(x[1])])

# Aggregate (user_id, category) keys in purchase RDD and filter out from search RDD
# Don't want to advertise to users who searched in a category and made the purchase
purchaseKeys = purchaseGroupRDD.keys().collect()
resultSearchRDD = searchGroupRDD.filter(lambda x: x[0] not in purchaseKeys).map(lambda x: [x[0], x[1]])


result = dict(resultSearchRDD.collect())

# Print key, values of result for testing
for k,v in result.iteritems():
    print "%s: %s" % (k, v)


