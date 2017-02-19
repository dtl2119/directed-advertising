#!/usr/bin/python

import time
import os
from kafka import KafkaConsumer


class Consumer(object):
    """
    Kafka Consumer class: consumes messages from a topic, and pushes to HDFS

    Messages blocked into 20MB files and put into HDFS in the directory:
    /user/web_logs

    Attributes:
        client: IP:port pair of kafka broker
        consumer: Consumer object specifying the client group/topic
        temp_file_path: Local on-disk path for temp_file, where  messages are accumulated
            transfer to HDFS
        temp_file: Actual temporary file where messages are  written locally 
            on disk until it reaches 20MB, at which point it's flushed to
            hdfs and then overwritten
        topic: Topic name the Consumer is subscribing to
        group: Kafka consumer group to be associated with
        block_cnt: Keep track of block number
    """
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = KafkaConsumer(bootstrap_servers = bootstrap_servers)
        self.consumer.subscribe([topic])
        self.hdfs_path = "/user/web_logs"
        self.block_num = 1
        #self.temp_file_path = None
        #self.temp_file = None
        #self.cached_path = "/user/cached" # FIXME
        #self.group = group

    def consume_topic(self, output_dir):
        """
        Consumes stream of messages from specified topic (in main)

        output_dir: Path where 20MB temp_file is, write to file before 
           flushing to HDFS
        """
        timestamp = time.strftime('%Y%m%d%H%M%S')
        
        # Write file locally (temporary)
        self.temp_file_path = "%s/kafka_%s_%s.csv" % (output_dir, self.topic, timestamp)
        self.temp_file = open(self.temp_file_path,"w")

        msg_count = 0
        for msg in self.consumer:
            msg_count += 1
            self.temp_file.write(msg.value + "\n")
            if msg_count % 1000 == 0:
                if self.temp_file.tell() > 20000000:
                    self.flush_to_hdfs(output_dir)



    def flush_to_hdfs(self, output_dir):
        self.temp_file.close()
        timestamp = time.strftime('%Y%m%d%H%M%S')
        hdfs_full_path = "%s/%s_%s.csv" % (self.hdfs_path, self.topic, timestamp)
        print "Block %s: Flushing temp 20MB file to hdfs (/user/web_logs)" % self.block_num
        self.block_num += 1

        hdfs_put_cmd = "hdfs dfs -copyFromLocal %s %s " % (self.temp_file_path, hdfs_full_path)
        os.system(hdfs_put_cmd)
        self.temp_file = open(self.temp_file_path, 'w')

if __name__ == '__main__':
    topic = "web_activity1"
    servers = ['localhost:9092']

    print "Consuming messages from topic %s . . ." % topic

    consumer = Consumer(bootstrap_servers=servers, topic=topic)

    consumer.consume_topic("tmp")

