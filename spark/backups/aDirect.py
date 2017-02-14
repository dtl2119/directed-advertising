
from cluster_ips import hdfs
from cluster_ips import cassandra
#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# TO RUN AS SPARK-SUBMIT JOB:
# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 --conf spark.cassandra.connection.host="Cassandra DB IPs" <pyspark_file>


def grabFromHDFS(filename):
    hdfs_master = hdfs['master1'] # Piublic IP of NamdeNode
    hdfs_port = "9000"
    full_hdfs_path = "hdfs://%s:%s/%s/%s" % (hdfs_master, hdfs_port, hdfs['base_dir'], filename)
    return spark.read.csv(full_hdfs_path)


def main():
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
    #filename = 'test_spark.csv'
    filename = 'small_batch_file.csv'
    #filename = 'output.csv'
    hdfs_master = hdfs['master1'] # Piublic IP of NamdeNode
    hdfs_port = "9000"
    full_hdfs_path = "hdfs://%s:%s/%s/%s" % (hdfs_master, hdfs_port, hdfs['base_dir'], filename)
    new_df =  spark.read.csv(full_hdfs_path)

    print "row count of new_df after grabbing from HDFS"
    print new_df.count()

    prev_df =  spark.read\
            .format("org.apache.spark.sql.cassandra")\
            .options(table="usersearches", keyspace="advertise")\
            .load()
    
    # Rename Columns of new DF
    old_cols = new_df.schema.names
    new_cols = ["time", "userid", "productid", "categoryid", "action"]
    new_df = reduce(lambda df, i: df.withColumnRenamed(old_cols[i], new_cols[i]), xrange(len(old_cols)), new_df)
    new_df.createOrReplaceTempView("new_table")
    print "row count of new_df after createorreplacetempview"
    print new_df.count()


    # Trim DataFrame to only relevant cols: user, category, product
    # Get DF of only searches (filter out users who made a purchase)
    searches_df = spark.sql("SELECT userid, categoryid, productid FROM new_table where action = 'search'")

    # Get DF of only buys from new DF to filter out of what was in Cassandra
    buys_df = spark.sql("SELECT userid, categoryid, productid FROM new_table where action = 'buy'")
    print "row count of buys and search df after SQL selecting the 3 fields"
    print buys_df.count()
    print searches_df.count()

    buys_df.createOrReplaceTempView("buys_table")
    
    # Remove from previous if they purchased
    prev_filtered_df = prev_df.join(buys_df, ['userid', 'categoryid'], 'left_anti')

    print "row count of prev_filtered_df, after left anti join"
    print prev_filtered_df.count()
    # Get new DF of searches in format of: [userid, categoryid]: [list, of, searches] 
    new_searches_df = searches_df.groupby('userid', 'categoryid').agg(F.collect_list('productid').alias('searches'))

    print "row count of new_searches_df after group by"
    print new_searches_df.count()

    result_df = prev_filtered_df.union(new_searches_df)
    print "row count of result_df after union:"
    print result_df.count()

    result_df.write\
            .format("org.apache.spark.sql.cassandra")\
            .mode('overwrite')\
            .options(table="usersearches", keyspace="advertise")\
            .save()

    return True


if __name__ == '__main__':

    # Use SparkSession builder to create a new session
    spark = SparkSession.builder.appName("adirect").getOrCreate()

    main()

    #writeToCassandra(result_df)

    spark.stop() # Stop spark session
