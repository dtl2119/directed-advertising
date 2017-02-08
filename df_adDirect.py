#!/usr/bin/python

from cluster_ips import hdfs
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# TO RUN AS SPARK-SUBMIT JOB:
# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 --conf spark.cassandra.connection.host="ec2-34-199-42-65.compute-1.amazonaws.com" df_adDirect.py

#df = session.read.csv("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/web_logs/small_batch_file.csv")
def grabFromHDFS(filename):
    hdfs_master = hdfs['master1'] # Piublic IP of NamdeNode
    hdfs_port = "9000"
    #filename = "*.csv"
    full_hdfs_path = "hdfs://%s:%s/%s/%s" % (hdfs_master, hdfs_port, hdfs['base_dir'], filename)
    #return session.read.csv("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/web_logs/small_batch_file.csv")
    return session.read.csv(full_hdfs_path)


def filterDF(sparksession, df):
    """
    This function takes the SparkSession and raw dataframe from HDFS and:
    - Names the columns, for query readability
    - Splits raw df of logs into 2: searches and buys
    """
    # Rename Columns of DF
    old_cols = df.schema.names
    new_cols = ["time", "userid", "productid", "categoryid", "action"]
    df = reduce(lambda df, i: df.withColumnRenamed(old_cols[i], new_cols[i]), xrange(len(old_cols)), df)

    # Trim DataFrame to only relevant cols: user, category, product
    df.createOrReplaceTempView("df_table")

    # Get DF of only searches (filter out users who made a purchase)
    df_searches = session.sql("SELECT userid, categoryid, productid FROM df_table where action = 'search'")

    # Get DF of only buys
    #df_buys = session.sql("SELECT userid, categoryid, productid FROM df_table where action = 'buy'")

    # Get in format of: [userid, categoryid]: [list, of, searches] 
    df_grouped_searches = df_searches.groupby('userid', 'categoryid').agg(F.collect_list('productid').alias('searches'))

    return df_grouped_searches


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
    session = SparkSession.builder.appName("adirect").getOrCreate()

    df = grabFromHDFS('small_batch_file.csv')

    resultDF = filterDF(session, df)

    # FIXME: For testing
    resultDF.show(3, False)

    writeToCassandra(resultDF)

    # Stop the underlying SparkContext
    session.stop()
