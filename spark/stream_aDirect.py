
import os, sys
sys.path.append("%s/directed-advertising/gitignored" % (os.environ['HOME']))
from cluster_ips import first, kafka
from cluster_ips import cassandra

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, Row
from pyspark.streaming.kafka import KafkaUtils


# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def writeToCassandra(table, df):
    """
    Connect to the Cassandra cluster (local) and write
    the dataframe results to the specified table
    """
    df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table=table, keyspace="advertise")\
        .save()


def process(rdd):
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())
      
        splitRDD = rdd.map(lambda l: l.split(","))
        namedRDD = splitRDD.map(lambda c: Row(time=c[0], user=c[1], userid=c[2], productid=c[3], categoryid=c[4], action=c[5]))
        stream_df = spark.createDataFrame(namedRDD)
        
        # Creates a temporary view using the DataFrame
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


        search_query = """
            SELECT userid, categoryid, time, productid, user
            FROM stream_table
            WHERE action = 'search'
            """

        searches_df = spark.sql(search_query)
        searches_df = searches_df.join(buys_df, ['userid', 'categoryid'], 'leftanti')

        searches_df.show()

        writeToCassandra("usersearches", searches_df)
        writeToCassandra("searches", searches_df)
        
    except:
        pass


        
if __name__ == "__main__":

    topics = ["web_activity1"]
    socket_format = "{}:{}"

    # Kafka Brokers, first is a dict of first cluster's IPs
    kafka_server_list = [socket_format.format(v, kafka['port']) for v in first.values()]
    bootstrap_servers = ", ".join(kafka_server_list)
   
    # Create StreamingContext object from SparkContext object
    sc = SparkContext(appName="stream_adirect")
    ssc = StreamingContext(sc, 1) 

    # Create DStream object
    kafkaStream = KafkaUtils.createDirectStream(ssc,
                                                topics, 
                                                {"bootstrap.servers": bootstrap_servers})

    # Returns DStream object (2nd element is the csv line)
    logs = kafkaStream.map(lambda x: x[1])
    
    logs.foreachRDD(process)
    
    ssc.start()
    ssc.awaitTermination()


