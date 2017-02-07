#!/usr/bin/python

from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F

df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load("/home/ubuntu/directed-advertising/small_batch_file.csv") # type: <class 'pyspark.sql.dataframe.DataFrame'>


# Rename Columns of DF
old_cols = df.schema.names
new_cols = ["time", "userid", "productid", "categoryid", "action"]
df = reduce(lambda df, i: df.withColumnRenamed(old_cols[i], new_cols[i]), xrange(len(old_cols)), df)

# Trim DataFrame to only relevant cols: user, category, product
df.createOrReplaceTempView("df_table")
#df_trimmed = spark.sql("SELECT userid, categoryid, productid FROM df_table")


# Get DF of only searches
df_searches = spark.sql("SELECT userid, categoryid, productid FROM df_table where action = 'search'")

# Get DF of only buys
df_searches = spark.sql("SELECT userid, categoryid, productid FROM df_table where action = 'buy'")

df_grouped_searches = df_searches.groupby('userid', 'categoryid').agg(F.collect_list('productid').alias('searches'))

df_grouped_searches.show(3, False)





##########


#
#from pyspark import SparkContext, SparkConf
#from cassandra.cluster import Cluster
#
## Connect to Cassandra cluster and create session
#cluster = Cluster()
#cluster = Cluster(['172.31.0.133']) # Only need 1 private ip, it will auto discover remaining nodes in cluster
#session = cluster.connect('advertise') # Connect to cluster, create session w/ keyspace = 'advertise'
#
## To submit this python application on Spark cluster for execution:
## $SPARK_HOME/bin/spark-submit <path_to_application> --master spark://<master_public_dns:7077
#
## The helper value sc is created in spark-shell, not automatically
## created in spark-submit: must instantiate own SparkContext to use
#conf = SparkConf().setAppName("adTarget")
#sc = SparkContext(conf=conf)
#
## For files in hdfs
##file = sc.textFile("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/test.txt")
#
## For files on disk (file must be on all nodes in the cluster)
#webLogRdd = sc.textFile("file:///home/ubuntu/directed-advertising/small_batch_file.txt/")
