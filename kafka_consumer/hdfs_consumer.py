#!/usr/bin/python


import time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
import os


class Consumer(object):
    def __init__(self, addr, group, topic):
        """Initialize Consumer with kafka broker IP, group, and topic."""
        self.client = KafkaClient(addr)
        self.consumer = SimpleConsumer(self.client, group, topic,
                                       max_buffer_size=1310720000)
        self.temp_file_path = None
        self.temp_file = None
        self.hadoop_path = "/user/directed-advertising/history"
        self.cached_path = "/user/directed-advertising/cached"
        self.topic = topic
        self.group = group
        self.block_cnt = 0

    def consume_topic(self, output_dir):
        timestamp = time.strftime('%Y%m%d%H%M%S')
        
        # open file for writing
        self.temp_file_path = "%s/kafka_%s_%s_%s.dat" % (output_dir,self.topic, self.group, timestamp)
        self.temp_file = open(self.temp_file_path,"w")

        while True:
            try:
                # Grab 10000 messages
                messages = self.consumer.get_messages(count=10000, block=False)

                # Write messages to temp file
                for message in messages:
                    self.temp_file.write(message.message.value + "\n")

                # Flush to hdfs once 120MB
                if self.temp_file.tell() > 120000000:
                    self.flush_to_hdfs(output_dir)

                self.consumer.commit()
            except:
                # move to tail of kafka topic if consumer is referencing
                # unknown offset
                self.consumer.seek(0, 2)


    def flush_to_hdfs(self, output_dir):
        """
        Flushes the file into two folders under
        hdfs://user/directed-advertising/{history, cached}
        """
        self.temp_file.close()
        timestamp = time.strftime('%Y%m%d%H%M%S')
        hadoop_fullpath = "%s/%s_%s_%s.dat" % (self.hadoop_path, self.group,self.topic, timestamp)
        cached_fullpath = "%s/%s_%s_%s.dat" % (self.cached_path, self.group,self.topic, timestamp)
        print "Block #{} flushing 120MB file to HDFS".format(str(self.block_cnt),hadoop_fullpath)
        self.block_cnt += 1

        # Write current block to history/cache dirs in hdfs
        os.system("sudo -u hdfs hdfs dfs -put %s %s" % (self.temp_file_path, hadoop_fullpath))
        os.system("sudo -u hdfs hdfs dfs -put %s %s" % (self.temp_file_path,cached_fullpath))
        os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S')

        self.temp_file_path = "%s/kafka_%s_%s_%s.dat" % (output_dir,self.topic,self.group,timestamp)
        self.temp_file = open(self.temp_file_path, "w")


if __name__ == '__main__':

    print "Consuming web logs (topic: web_activity1): . . ."
    cons = Consumer(addr="localhost:9092", group="hdfs", topic="web_activity1")
    cons.consume_topic("/home/ubuntu/directed-advertising/kafka_consumer/kafka_msgs")

