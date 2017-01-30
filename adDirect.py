#!/usr/bin/python

import aws_creds
from pyspark import SparkContext, SparkConf

# sc is a helper value created in the spark-shell, 
# but not automatically created with spark-submit. 
# You must instantiate your own SparkContext and use that
conf = SparkConf().setAppName("adTarget")
sc = SparkContext(conf=conf)

# For files in hdfs
#file = sc.textFile("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/test.txt")

# Word Count Example
#counts = file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
#res = counts.collect()
#for val in res:
#     print val


# Function to parse lines of searches and purchases (CSVs)
# Timestamp, user_id (int), product_id (int), category_id (int)
def parseLines(line):
    time, userid, product, category = line.split(", ")
    return (int(userid),int(category),int(product))


# For files on disk
searchRDD = sc.textFile("file:///home/ubuntu/directed-advertising/short_search_batch.txt/")
purchaseRDD = sc.textFile("file:///home/ubuntu/directed-advertising/short_purchase_batch.txt/")

# Split, create list of lists
searchMap = searchRDD.map(lambda line: line.split(", "))
purchaseMap = purchaseRDD.map(lambda line: line.split(", "))


# WANT: (user, category) = [list, of, searches]
userToProduct = parseSearch.map(lambda n: (n[1], n[2])).groupByKey()
#result = userToProduct.map(lambda x: {x[0]: list(x[1])}).collect() # EX: {u'131': [u'2215', u'225', u'2210', u'2230']}


