#!/usr/bin/python


import time
#from kafka.client import KafkaClient
from kafka import KafkaClient
from kafka.consumer import SimpleConsumer
import os
import subprocess


# Code template from https://github.com/ajmssc/bitcoin-inspector.git

class Consumer(object):
    """
    Kafka Consumer class: consumes messages from a topic, and pushes to HDFS

    Messages blocked into 120MB files and put into HDFS.  Two directories:
    /user/web_logs: where web activity log files are stored
    /user/cached: files stored temporarily, overwritten on new batch job # FIXME

    Attributes:
        client: IP:port pair of kafka broker
        consumer: Consumer object specifying the client group/topic
        temp_file_path: Local on-disk path for temp_file, where  messages are accumulated
            transfer to HDFS
        temp_file: Actual temporary file where messages are  written locally 
            on disk until it reaches 120MB, at which point it's flushed to
            hdfs and then overwritten
        topic: Topic name the Consumer is subscribing to
        group: Kafka consumer group to be associated with
        block_cnt: Keep track of block number
    """
    def __init__(self, addr, group, topic):
        self.client = KafkaClient(addr)
        self.consumer = SimpleConsumer(self.client, group, topic,
                                       max_buffer_size=1310720000)
        self.temp_file_path = None
        self.temp_file = None
        self.hadoop_path = "/user/web_logs"
        #self.cached_path = "/user/cached" # FIXME
        self.topic = topic
        self.group = group
        self.block_cnt = 0

    def consume_topic(self, output_dir):
        """
        Consumes stream of messages from specified topic (in main)

        output_dir: Path where 120MB temp_file is, write to file before 
           flushing to HDFS
        """
        timestamp = time.strftime('%Y%m%d%H%M%S')
        
        # Write file locally (temporary)
        self.temp_file_path = "%s/kafka_%s_%s_%s.csv" % (output_dir,self.topic, self.group, timestamp)
        self.temp_file = open(self.temp_file_path,"w")

        while True:
            try:
                # get 1000 messages at a time, non blocking
                messages = self.consumer.get_messages(count=5000, block=False)

                # OffsetAndMessage(offset=43, message=Message(magic=0, # FIXME
                # attributes=0, key=None, value='some message')) # FIXME
                for message in messages:
                    self.temp_file.write(message.message.value + "\n")

                # file size > 120MB
                if self.temp_file.tell() > 120000000:
                    self.flush_to_hdfs(output_dir)

                self.consumer.commit()
            except:
                # move to tail of kafka topic if consumer is referencing
                # unknown offset
                self.consumer.seek(0, 2)


    def flush_to_hdfs(self, output_dir):
        """
        Flush 120MB file 

        Flushes the file to two folders:
        hdfs://user/web_logs
        hdfs://user/cached #FIXME

        output_dir: Path where  120MB file before transferring
                to HDFS

        """
        self.temp_file.close()
        timestamp = time.strftime('%Y%m%d%H%M%S')
        hadoop_fullpath = "%s/%s_%s_%s.csv" % (self.hadoop_path, self.topic, timestamp) # FIXME NOW
        #cached_fullpath = "%s/%s_%s_%s.csv" % (self.cached_path, self.group,self.topic, timestamp)

        print "Block #{}: Flushing 120MB file to HDFS => {}".format(str(self.block_cnt),hadoop_fullpath)
        self.block_cnt += 1

        # place blocked messages into history and cached folders on hdfs
#        os.system("sudo -u ubuntu hdfs dfs -put %s %s" % (self.temp_file_path,hadoop_fullpath)) # FIXME
#        os.system("sudo -u ubuntu hdfs dfs -put %s %s" % (self.temp_file_path,cached_fullpath)) # FIXME
        os.system("hdfs dfs -put %s %s" % (self.temp_file_path,hadoop_fullpath))
        #os.system("hdfs dfs -put %s %s" % (self.temp_file_path,cached_fullpath)) # FIXME

        os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S')

        self.temp_file_path = "%s/kafka_%s_%s_%s.csv" % (output_dir,self.topic, timestamp) #FIXME NOW (btwn topic and timestamp

        self.temp_file = open(self.temp_file_path, "w")


if __name__ == '__main__':

    print "Consuming web logs (topic: web_activity1): . . ."
    kafka_msgs_path = "%s/kafka_msgs" % (os.path.dirname(os.path.realpath(__file__)))
    cons = Consumer(addr="localhost:9092", group = "group",  topic="web_activity1")
    cons.consume_topic(kafka_msgs_path)

