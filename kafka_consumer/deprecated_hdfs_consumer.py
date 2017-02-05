#!/usr/bin/python


import time
#from kafka.client import KafkaClient
from kafka import KafkaClient
from kafka.consumer import SimpleConsumer
import os


class Consumer(object):
    """Kafka consumer class with functions to consume messages to HDFS.

    Messages blocked into 120MB files and put into HDFS.  Two directories:
    /user/web_logs: where web activity log files are stored
    /user/cached: files stored temporarily, overwritten on new batch job

    Attributes:
        client: IP:port pair of kafka broker
        consumer: Consumer object specifying the client group/topic
        temp_file_path: Location of the 120MB file to be appended to before
            transfer to HDFS
        temp_file: File object opened from temp_file_path
        topic: String representing the topic on Kafka
        group: Represents the Kafka consumer group to be associated with
        block_cnt: Keep track of block number
    """
    def __init__(self, addr, group, topic):
        self.client = KafkaClient(addr)
        self.consumer = SimpleConsumer(self.client, group, topic,
                                       max_buffer_size=1310720000)
        self.temp_file_path = None
        self.temp_file = None
        self.hadoop_path = "/user/web_logs"
        self.cached_path = "/user/cached"
        self.topic = topic
        self.group = group
        self.block_cnt = 0

    def consume_topic(self, output_dir):
        """Consumes a stream of messages from the "messages" topic.

        Code template from https://github.com/ajmssc/bitcoin-inspector.git

        Args:
            output_dir: Path to store the 120MB block before sending to HDFS

        """
        timestamp = time.strftime('%Y%m%d%H%M%S')
        
        # open file for writing
        self.temp_file_path = "%s/kafka_%s_%s_%s.dat" % (output_dir,
                                                         self.topic,
                                                         self.group,
                                                         timestamp)
        self.temp_file = open(self.temp_file_path,"w")

        while True:
            try:
                # get 1000 messages at a time, non blocking
                messages = self.consumer.get_messages(count=1000, block=False)

                # OffsetAndMessage(offset=43, message=Message(magic=0,
                # attributes=0, key=None, value='some message'))
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
        """Flushes the 120MB file to HDFS.

        Code template from https://github.com/ajmssc/bitcoin-inspector.git

        Flushes the file to two folders:
        hdfs://user/web_logs
        hdfs://user/cached

        Args:
            ourput_dir: directory path to store 120MB file before transferring
                to HDFS

        """
        self.temp_file.close()
        timestamp = time.strftime('%Y%m%d%H%M%S')
        hadoop_fullpath = "%s/%s_%s_%s.dat" % (self.hadoop_path, self.group,
                                               self.topic, timestamp)
        cached_fullpath = "%s/%s_%s_%s.dat" % (self.cached_path, self.group,
                                               self.topic, timestamp)
        print "Block #{}: Flushing 120MB file to HDFS => {}".format(str(self.block_cnt),
                                                                  hadoop_fullpath)
        self.block_cnt += 1

        # place blocked messages into history and cached folders on hdfs
        os.system("sudo -u hdfs hdfs dfs -put %s %s" % (self.temp_file_path,
                                                        hadoop_fullpath))
        os.system("sudo -u hdfs hdfs dfs -put %s %s" % (self.temp_file_path,
                                                        cached_fullpath))
        os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S')

        self.temp_file_path = "%s/kafka_%s_%s_%s.dat" % (output_dir,
                                                         self.topic,
                                                         self.group,
                                                         timestamp)
        self.temp_file = open(self.temp_file_path, "w")


if __name__ == '__main__':

    print "Consuming web logs (topic: web_activity1): . . ."
    cons = Consumer(addr="localhost:9092", group="hdfs", topic="web_activity1")
    cons.consume_topic("/home/ubuntu/directed-advertising/kafka_consumer/kafka_msgs")

