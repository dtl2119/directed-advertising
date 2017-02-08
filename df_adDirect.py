#!/usr/bin/python

from cluster_ips import hdfs
from pyspark import SparkContext, SparkConf
#from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


session = SparkSession.builder.appName("adirect").getOrCreate()

#df = session.read.csv("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/web_logs/small_batch_file.csv")
def grabFromHDFS(filename):
    hdfs_master = hdfs['master1'] # Piublic IP of NamdeNode
    hdfs_port = "9000"
    full_hdfs_path = "hdfs://%s:%s/%s/%s" % (hdfs_master, hdfs_port, hdfs['base_dir'], filename)
    #return session.read.csv("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/web_logs/small_batch_file.csv")
    return session.read.csv(full_hdfs_path)

#conf = SparkConf().setAppName("adirect")
#sc = SparkContext(conf=conf)
# For files in hdfs
#file = sc.textFile("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/test.txt")
#raw_file = session.sparkContext.textFile("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/web_logs/small_batch_file.csv")
#df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load("/home/ubuntu/directed-advertising/small_batch_file.csv") # type: <class 'pyspark.sql.dataframe.DataFrame'>

df = grabFromHDFS('small_batch_file.csv')

# Rename Columns of DF
old_cols = df.schema.names
new_cols = ["time", "userid", "productid", "categoryid", "action"]
df = reduce(lambda df, i: df.withColumnRenamed(old_cols[i], new_cols[i]), xrange(len(old_cols)), df)

# Trim DataFrame to only relevant cols: user, category, product
df.createOrReplaceTempView("df_table")
#df_trimmed = spark.sql("SELECT userid, categoryid, productid FROM df_table")

# Get DF of only searches
df_searches = session.sql("SELECT userid, categoryid, productid FROM df_table where action = 'search'")

# Get DF of only buys
df_buys = session.sql("SELECT userid, categoryid, productid FROM df_table where action = 'buy'")

df_grouped_searches = df_searches.groupby('userid', 'categoryid').agg(F.collect_list('productid').alias('searches'))

df_grouped_searches.show(3, False)

#session.sql("INSERT INTO advertise.usersearches (userid, categoryid, searches) VALUES ('2', '2', ['2', '2'])")
#df_grouped_searches.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='usersearches', keyspace='advertise').save()
#ed_searches.select('userid', 'categoryid', 'searches').write.save
