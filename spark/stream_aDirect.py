
import os, sys
sys.path.append("%s/directed-advertising/gitignored" % (os.environ['HOME']))
from cluster_ips import first, kafka
from cluster_ips import cassandra

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils


# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process(rdd):
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        #df = spark.read.json(rdd)
        stream_df = spark.read.csv(rdd)

        old_cols = stream_df.schema.names
        new_cols = ["time", "user", "userid", "productid", "categoryid", "action"]
        stream_df = reduce(lambda df, i: df.withColumnRenamed(old_cols[i], new_cols[i]), xrange(len(old_cols)), stream_df)
        stream_df.createOrReplaceTempView("stream_table")

        # For buys: select where buys were made, then groupby productid and count
        # frequency --> indicator of product popularity
        buy_query = """
            SELECT userid, categoryid, productid
            FROM stream_table
            WHERE action = 'buy'
            """
        buys_df = spark.sql(buy_query)
        grouped_buys_df = buys_df.groupby(buys_df.productid).count()

        # Creates a temporary view using the DataFrame

        search_query = """
            SELECT userid, categoryid, time, productid, user
            FROM stream_table
            WHERE action = 'search'
            """

        searches_df = spark.sql(search_query)
        searches_df = searches_df.join(buys_df, ['userid', 'categoryid'], 'left_anti')

        
        searches_df.show()
        searches_df.w
        
        writeToCassandra("usersearches", searches_df)
        
    except:
        pass


def writeToCassandra(table, df):
    """
    Connect to the Cassandra cluster (local) and write
    the dataframe results to the specified table
    """
    df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=table, keyspace="advertise", cluster = "de-ny-drew2")\
        .save()
        

if __name__ == "__main__":

    topics = ["web_activity1"]
    #socket_format = "{}:{}"
    #bootstrap_servers = [socket_format.format(v, kafka['port']) for v in first.values()]
    bootstrap_servers = "%s:%s" % (kafka['master1'], kafka['port'])
   
    # Create StreamingContext object from SparkContext object
    sc = SparkContext(appName="stream_adirect")
    ssc = StreamingContext(sc, 1) 

    # Create DStream object
    kafkaStream = KafkaUtils.createDirectStream(ssc,
                                                topics, 
                                                {"bootstrap.servers": bootstrap_servers})
    
    raw_logs = kafkaStream.map(lambda x: x[0])
#    with open("raw_logs_variable.txt", 'w') as debugFile:
#        debugFile.write(raw_logs.pprint(5)+"\n")
    
    raw_logs.foreachRDD(process)
    
    # Start actual process
    ssc.start()

    # Wait for process to finish
    ssc.awaitTermination()


