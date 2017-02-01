#!/usr/bin/python

import argparse
import random
import sys
import six #FIXME: needed?
import time
from datetime import datetime
from faker import Faker
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
from random import randint

# FIXME: Add to argparse, make output arguments
search_file_path = "/Users/drewlimm/insight/directed-advertising/short_search_batch.txt"
purchase_file_path = "/Users/drewlimm/insight/directed-advertising/short_purchase_batch.txt"

# FIXME: append larger "category id" to beginning
tvs = [int(str(11)+ str(tv)) for tv in range(0, (5+1)*10, 10)[1:]] # tv ids: 5 tvs, multiple of 10, append 1 to front of number
cables = [int(str(22)+ str(cable)) for cable in range(0, (10+1)*5, 5)[1:]] # cables ids: 10 cables, multiple of 5, append 2 to front of number
cameras = [int(str(33)+ str(camera)) for camera in range(0, (15+1)*3, 3)[1:]] # cables ids: 15 cameras, multiple of 3, append 3 to front of number
computers = [int(str(44)+ str(comp)) for comp in range(0, (20+1)*4, 4)[1:]] # cables ids: 20 computers, multiple of 4, append 4 to front of number

categories = {1:tvs, 2: cables, 3:cameras, 4:computers}

parser = argparse.ArgumentParser(__file__, description="Generate user search and purchase history")
parser.add_argument("--num", "-n", dest='num_lines', help="Number of lines to generate (0 for infinite)", type=int, default=1)

args = parser.parse_args()
logLines = args.num_lines

# Format of messages to be sent: csv
msg_fmt = "{},{},{},{}"

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_symbol):
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

            # Pick 2 random users (index)
            userIndex1 = randint(0, len(userSet)-1)
            userIndex2 = userIndex1 + 1
            # Avoid index out of bounds
            if userIndex2 > len(userSet)-1:
                userIndex2 -= 2

            user1 = userSet[userIndex1]
            user2 = userSet[userIndex2]

            for x in range(20): # Max 20 searches for each user (less if they purchase) 

                now = datetime.now().strftime('%d/%b/%Y %H:%M:%S')
                #epoch = time.time() * 1000
                #epoch = time.time()

                user1ProductId = random.choice(tvs)
                user2ProductId = random.choice(cables)
                    
                user1SearchMsg = msg_fmt.format(now,user1,user1ProductId,1)
                user2SearchMsg= msg_fmt.format(now,user2,user2ProductId,2)
                logLines -= 2

                # WRITE SEARCH
                #searchFile.write("%s, %s, %s, %s \n" % (now, user1, user1ProductId, 1))
                #searchFile.write("%s, %s, %s, %s \n" % (now, user2, user2ProductId, 2))
                #logLines -= 2

                # WRITE PURCHASE
                if randint(1,20) == 1: # 5% Chance of buying
                    purchaseFile.write("%s, %s, %s, %s\n" % (now, user1, user1ProductId, 1))
                    break

                if randint(1,20) == 1: # 5% Chance of buying
                    purchaseFile.write("%s, %s, %s, %s\n" % (now, user2, user2ProductId, 2))
                    break
                
                if logLines <= 0:
                    break

                
            flag = False if logLines <= 0 else True


if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key) 



### From my script

# For each producer/process, change range
# Ex: process1: userids = 1 to 1000
#     process2: userids = 1001 to 2000
#     process3: userids = 2001 to 3000
num_user = 1000 # Number of unique users
userSet = random.sample(range(1, num_user+1), num_user)


with open(search_file_path, "w") as searchFile, open(purchase_file_path, "w") as purchaseFile:
    flag = True
    while (flag):

        # Pick 2 random users (index)
        userIndex1 = randint(0, len(userSet)-1)
        userIndex2 = userIndex1 + 1
        # Avoid index out of bounds
        if userIndex2 > len(userSet)-1:
            userIndex2 -= 2

        user1 = userSet[userIndex1]
        user2 = userSet[userIndex2]

        for x in range(20): # Max 20 searches for each user (less if they purchase) 

            now = datetime.now().strftime('%d/%b/%Y:%H:%M:%S')
            #epoch = time.time() * 1000
            #epoch = time.time()

            user1ProductId = random.choice(tvs)
            user2ProductId = random.choice(cables)
                
            # WRITE SEARCH
            searchFile.write("%s, %s, %s, %s \n" % (now, user1, user1ProductId, 1))
            searchFile.write("%s, %s, %s, %s \n" % (now, user2, user2ProductId, 2))
            logLines -= 2

            # WRITE PURCHASE
            if randint(1,20) == 1: # 5% Chance of buying
                purchaseFile.write("%s, %s, %s, %s \n" % (now, user1, user1ProductId, 1))
                break

            if randint(1,20) == 1: # 5% Chance of buying
                purchaseFile.write("%s, %s, %s, %s \n" % (now, user2, user2ProductId, 2))
                break
            
            if logLines <= 0:
                break

            
        flag = False if logLines <= 0 else True
