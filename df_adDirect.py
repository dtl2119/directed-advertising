#!/usr/bin/python

from cluster_ips import hdfs
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# TO RUN AS SPARK-SUBMIT JOB:
# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 --conf spark.cassandra.connection.host="ec2-34-199-42-65.compute-1.amazonaws.com" <pyspark_file>

#df = spark.read.csv("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/web_logs/small_batch_file.csv")
def grabFromHDFS(filename):
    hdfs_master = hdfs['master1'] # Piublic IP of NamdeNode
    hdfs_port = "9000"
    full_hdfs_path = "hdfs://%s:%s/%s/%s" % (hdfs_master, hdfs_port, hdfs['base_dir'], filename)
    #return spark.read.csv("hdfs://ec2-34-198-20-105.compute-1.amazonaws.com:9000/user/web_logs/small_batch_file.csv")
    return spark.read.csv(full_hdfs_path)


def processNewDF(spark, new_df):
    """
    Function takes two parameters: 
    1) The spark session
    2) New raw dataframe (from HDFS)

    It creates a df from previous results, stored in a Cassandra cluster.
    Then it names the columns of the new/incoming df for query readability,
    and splits the df into 2 separate dfs: searches and buys.  
    The new buys are used to filter out user, category pairs from the Cassandra 
    data df (don't advertise to users who made a purchase).  Once filtered, 
    union the new searches, and return the resulting DF.

    """
    prev_df = grabFromCassandra(spark) # Data to be updated
    
    # Rename Columns of new DF
    old_cols = new_df.schema.names
    new_cols = ["time", "userid", "productid", "categoryid", "action"]
    new_df = reduce(lambda df, i: df.withColumnRenamed(old_cols[i], new_cols[i]), xrange(len(old_cols)), new_df)
    new_df.createOrReplaceTempView("new_table")


    # Trim DataFrame to only relevant cols: user, category, product
    # Get DF of only searches (filter out users who made a purchase)
    searches_df = spark.sql("SELECT userid, categoryid, productid FROM new_table where action = 'search'")

    # Get DF of only buys from new DF to filter out of what was in Cassandra
    buys_df = spark.sql("SELECT userid, categoryid, productid FROM new_table where action = 'buy'")

    buys_df.createOrReplaceTempView("buys_table")
    prev_df.createOrReplaceTempView("prev_table")
    
    # Remove from previous if they purchased
    prev_filtered_df = prev_df.join(buys_df, ['userid', 'categoryid'], 'left_anti')

    # Get new DF of searches in format of: [userid, categoryid]: [list, of, searches] 
    new_searches_df = searches_df.groupby('userid', 'categoryid').agg(F.collect_list('productid').alias('searches'))

    result_df = prev_filtered_df.union(new_searches_df)

    return result_df


def grabFromCassandra(spark):
    """
    Take a SparkSession and connect to local Cassandra DB (de-ny-drew2)

        Returns: DF containing results from the previous spark-submit run
    """

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

    #new_df = grabFromHDFS('test_spark.csv')
    new_df = grabFromHDFS('small_batch_file.csv')
    result_df = processNewDF(spark, new_df)

    writeToCassandra(result_df)

    spark.stop() # Stop spark session
