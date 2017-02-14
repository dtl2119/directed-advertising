
import sys
sys.path.append('/ufs/guido/lib/python')
from cluster_ips import hdfs
from cluster_ips import cassandra
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Running as spark-submit
# spark-submit 
# --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3
# --conf spark.cassandra.connection.host="Cassandra DB IPs" <pyspark_file>

def grabFromHDFS(filename = "*.csv"):
    """
    Grab csv file(s) from hdfs cluster on de-ny-drew
    If no filename provided, grab all csv files from 
    base directory in hdfs: /user/web_logs

    Returns:
        Data frame split by comma delimiter

    """
    hdfs_master = hdfs['master1'] # Public IP of NamdeNode
    hdfs_port = hdfs['port'] # 9000
    full_hdfs_path = "hdfs://%s:%s/%s/%s" % (hdfs_master, hdfs_port, hdfs['base_dir'], filename)
    return spark.read.csv(full_hdfs_path)


def writeToCassandra(table, df):
    """
    Connect to the Cassandra cluster (local) and write
    the dataframe results to the specified table
    """
    df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('overwrite')\
        .options(table=table, keyspace="advertise")\
        .save()

def main(batch_df):
    """
    Function uses the existing spark session and takes a dataframe;
    parsed csv file(s) grabbed from a HDFS cluster (not local, from
    first cluster)

        First, it  renames columns for readability.  Then it creates a
        temp view to run SQL on the table.  Uses Spark engine to:
        1) Run SQL to filter (searches and buys)
        2) Groupby,then aggregate search list and calculate buy count

        Returns 2 key/value dataframes:
            searches --> (userid, categoryid): set(list, of searches)
            buys     --> productid: buy count
    """
    # Rename Columns of new DF
    old_cols = batch_df.schema.names
    new_cols = ["time", "user", "userid", "productid", "categoryid", "action"]
    batch_df = reduce(lambda df, i: df.withColumnRenamed(old_cols[i], new_cols[i]), xrange(len(old_cols)), batch_df)
    batch_df.createOrReplaceTempView("new_table")

    # For buys: select where buys were made, then groupby productid and count
    # frequency --> indicator of product popularity
    buy_query = """
        SELECT userid, categoryid, productid
        FROM new_table
        WHERE action = 'buy'
        """
    buys_df = spark.sql(buy_query)
    grouped_buys_df = buys_df.groupby(buys_df.productid).count()


    # For searches: select all search log activity, do an antijoin with
    # the buys_df to remove users who purchased, then group by tuple and
    # aggregate searches:
    # (userid, categoryid): [list, of, searches] 
    search_query = """
        SELECT userid, categoryid, time, productid, user
        FROM new_table
        WHERE action = 'search'
        """
    searches_df = spark.sql(search_query)
    searches_df = searches_df.join(buys_df, ['userid', 'categoryid'], 'left_anti')

    # Uncomment to group by (userid, categoryid) and value = list of searches
    #grouped_searches_df = searches_df.groupby('userid', 'categoryid').agg(F.collect_list('productid').alias('searches'))

    return searches_df, grouped_buys_df


if __name__ == '__main__':

    # Use SparkSession builder to create a new session
    spark = SparkSession.builder.appName("batch_adirect").getOrCreate()

    # Specify filename for testing (default = *.csv)
    filename = 'output.csv'
    hdfs_df = grabFromHDFS(filename)

    searches_result_df, buys_result_df = main(hdfs_df)

    writeToCassandra("usersearches", searches_result_df)
    writeToCassandra("userbuys", buys_result_df)

    spark.stop()
