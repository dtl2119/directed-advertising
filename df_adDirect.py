#!/usr/bin/python

from cluster_ips import hdfs
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# TO RUN AS SPARK-SUBMIT JOB:
# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 --conf spark.cassandra.connection.host="ec2-34-199-42-65.compute-1.amazonaws.com" df_adDirect.py

#df = spark.read.csv("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/web_logs/small_batch_file.csv")
def grabFromHDFS(filename):
    hdfs_master = hdfs['master1'] # Piublic IP of NamdeNode
    hdfs_port = "9000"
    full_hdfs_path = "hdfs://%s:%s/%s/%s" % (hdfs_master, hdfs_port, hdfs['base_dir'], filename)
    #return spark.read.csv("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/web_logs/small_batch_file.csv")
    return spark.read.csv(full_hdfs_path)


def filterDF(spark, df):
    """
    This funtion takes two parameters: 
    1) The spark session
    2) Raw dataframe (from HDFS)

    Then it names the columns for query readability, and splits the df into
    two separate dfs: searches and buys
    """

    # Rename Columns of DF
    old_cols = df.schema.names
    new_cols = ["time", "userid", "productid", "categoryid", "action"]
    df = reduce(lambda df, i: df.withColumnRenamed(old_cols[i], new_cols[i]), xrange(len(old_cols)), df)

    # Trim DataFrame to only relevant cols: user, category, product
    df.createOrReplaceTempView("df_table")

    # Get DF of only searches (filter out users who made a purchase)
    df_searches = spark.sql("SELECT userid, categoryid, productid FROM df_table where action = 'search'")

    # Get DF of only buys
    #df_buys = spark.sql("SELECT userid, categoryid, productid FROM df_table where action = 'buy'")

    # Get in format of: [userid, categoryid]: [list, of, searches] 
    df_grouped_searches = df_searches.groupby('userid', 'categoryid').agg(F.collect_list('productid').alias('searches'))

    return df_grouped_searches

def grabFromCassandra(spark):
    """
    Takes a SparkSession and connects to local Cassandra DB (de-ny-drew2)
    Returns DF containing results from the previous spark-submit run
    """
    #df = sqlContext.read\
    return spark.read\
            .format("org.apache.spark.sql.cassandra")\
            .options(table="usersearches", keyspace="advertise")\
            .load()
    

def writeToCassandra(df):
    """
    Write resulting dataframe to the Cassandra DB
    """
    df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('append')\
            .options(table="usersearches", keyspace="advertise")\
            .save()


if __name__ == '__main__':

    # Use SparkSession builder to create a new session
    spark = SparkSession.builder.appName("adirect").getOrCreate()

    previous_df = grabFromCassandra(spark) # Data to be updated
    print "hello"
    print type(previous_df)
    print "world"
    previous_df.show(5, False)
    import sys
    sys.exit(3)

    df = grabFromHDFS('small_batch_file.csv')

    resultDF = filterDF(spark, df)

    # FIXME: For testing
    resultDF.show(3, False)

    writeToCassandra(resultDF)

    # Stop the underlying SparkContext
    spark.stop()
