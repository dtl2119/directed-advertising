#!/usr/bin/python

import random
import sys
import six #FIXME: needed?
import time
from datetime import datetime
from faker import Faker
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer


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

    def produce_msgs(self):
        while (True):

            # Pick 3 random users (index)
            random.shuffle(userSet)
            # user_id1, user_id2, user_id3 = userSet[:3]
            currUsers = userSet[:3]
           
            # Tuple for users, ensure searching in same category
            # (user, category_id)
            userTuples = []
            for user in currUsers:
                cat_id = random.choice(categories.keys())
                userTuples.append((user, cat_id))

            for x in range(20): # Max 20 searches for each user (less if they purchase) 
                now = datetime.now().strftime('%d/%b/%Y %H:%M:%S') # For epoch --> time.time()

                # For each (user, category), pick a product and action (either 'search' or 'buy')
                for tup in userTuples:
                    product_id = random.choice(categories[tup[1]])
		    ranNum = random.randint(1,20)
                    action = "buy" if ranNum == 1 else "search" # 5% chance of buying
                    userMsg = msg_fmt.format(now, tup[0], product_id, tup[1], action)
                    #print userMsg # FIXME
                    time.sleep(0.05)
                    self.producer.send_messages('web_activity1', str(ranNum), userMsg) # Where 'web_activity1' is the topic
                    if action == "buy":
                        break


                # Get 3 new users if a purchase made
                if action == "buy":
                    break


if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    #partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs() 


