#!/usr/bin/python

import random
import sys
import six #FIXME: needed?
import time
from datetime import datetime
from faker import Faker
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
#from random import randint
#from random import shuffle

# FIXME: Add to argparse, make output arguments
search_file_path = "/Users/drewlimm/insight/directed-advertising/short_search_batch.txt"
purchase_file_path = "/Users/drewlimm/insight/directed-advertising/short_purchase_batch.txt"

# FIXME: append larger "category id" to beginning
tvs = [int(str(11)+ str(tv)) for tv in range(0, (5+1)*10, 10)[1:]] # tv ids: 5 tvs, multiple of 10, append 1 to front of number
cables = [int(str(22)+ str(cable)) for cable in range(0, (10+1)*5, 5)[1:]] # cables ids: 10 cables, multiple of 5, append 2 to front of number
cameras = [int(str(33)+ str(camera)) for camera in range(0, (15+1)*3, 3)[1:]] # cables ids: 15 cameras, multiple of 3, append 3 to front of number
computers = [int(str(44)+ str(comp)) for comp in range(0, (20+1)*4, 4)[1:]] # cables ids: 20 computers, multiple of 4, append 4 to front of number

categories = {1:tvs, 2: cables, 3:cameras, 4:computers}


# Total number of users
numUsers = 5000 
userSet = random.sample(range(1, numUsers+1), numUsers) # ensure userids are unique


# Format of messages to be sent: csv
msg_fmt = "{},{},{},{},{}"

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

#    def produce_msgs(self, source_symbol): # FIXME
def produce_msgs():
#        price_field = random.randint(800,1400)
#        msg_cnt = 0
#        while True:
#            time_field = datetime.now().strftime("%Y%m%d %H%M%S")
#            price_field += random.randint(-10, 10)/10.0
#            volume_field = random.randint(1, 1000)
#            str_fmt = "{};{};{};{}"
#            message_info = str_fmt.format(source_symbol,
#                                          time_field,
#                                          price_field,
#                                          volume_field)
#            print message_info
#            self.producer.send_messages('price_data_part4', source_symbol, message_info)
#            msg_cnt += 1

        flag = True
        while (flag):

            # Pick 3 random users (index)
            random.shuffle(userSet)
            # user_id1, user_id2, user_id3 = userSet[:3] # FIXME
            currUsers = userSet[:3]
            
            msgs = []

            for x in range(20): # Max 20 searches for each user (less if they purchase) 
                

                # For each user: pick a random category, product in that category, and 
                # an action ("search" or "buy")
                for user_id in currUsers:
                    now = datetime.now().strftime('%d/%b/%Y %H:%M:%S') # For epoch --> time.time()
                    cat_id = random.choice(categories.keys())
                    product_id = random.choice(categories[cat_id])
                    action = "buy" if random.randint(1,20) == 1 else "search" # 5% chance of buying
                    userMsg = msg_fmt.format(now, user_id, product_id, cat_id, action)
                    msgs.append(userMsg)
                    #logLines -= 1 # FIXME
                    if action == "buy":
                        break
            
                #if logLines <= 0: # FIXME
                #    break
            
            for m in msgs:
                print m
            time.sleep(1)
                
            #flag = False if logLines <= 0 else True # FIXME


if __name__ == "__main__":
#    args = sys.argv
#    ip_addr = str(args[1])
#    partition_key = str(args[2])
#    prod = Producer(ip_addr)
#    prod.produce_msgs(partition_key) 

    produce_msgs()



### Insight Example:
#    def produce_msgs(self, source_symbol):
#        price_field = random.randint(800,1400)
#        msg_cnt = 0
#        while True:
#            time_field = datetime.now().strftime("%Y%m%d %H%M%S")
#            price_field += random.randint(-10, 10)/10.0
#            volume_field = random.randint(1, 1000)
#            str_fmt = "{};{};{};{}"
#            message_info = str_fmt.format(source_symbol,
#                                          time_field,
#                                          price_field,
#                                          volume_field)
#            print message_info
#            self.producer.send_messages('price_data_part4', source_symbol, message_info)
#            msg_cnt += 1
