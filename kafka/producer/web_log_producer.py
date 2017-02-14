#!/usr/bin/python

import random
import sys
import six #FIXME: needed?
import time
from users_dict import userDict
from datetime import datetime
from faker import Faker
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer


# Product id lists
tvs = [int(str(111)+ str(x)) for x in range(10000)]
cables = [int(str(222)+ str(x)) for x in range(10000)]
cameras = [int(str(333)+ str(x)) for x in range(10000)]
phones = [int(str(444)+ str(x)) for x in range(10000)]
computers = [int(str(555)+ str(x)) for x in range(10000)]
memory = [int(str(666)+ str(x)) for x in range(10000)]
monitors = [int(str(777)+ str(x)) for x in range(10000)]
audio = [int(str(888)+ str(x)) for x in range(10000)]
chargers = [int(str(999)+ str(x)) for x in range(10000)]
misc = [int(str(100)+ str(x)) for x in range(10000)]

# cables ids: 20 computers, multiple of 4, append 4 to front of number
computers = [int(str(444)+ str(comp)) for comp in range(0, (20+1)*4, 4)[1:]] 

categories = {
    1: tvs, 
    2: cables, 
    3: cameras, 
    4: phones,
    5: computers,
    6: memory,
    7: monitors,
    8: audio,
    9: chargers,
    10: misc
}


# Total number of users
#numUsers = 50000 # FIXME
#userSet = random.sample(range(1, numUsers+1), numUsers) # ensure userids are unique # FIXME


# Format of messages to be sent: csv
msg_fmt = "{},{},{},{},{},{}"

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self):
        while (True):

            # Pick 5 random user ids
            currUsers = random.sample(userDict.keys(), 5)
           
            # Tuple for users, ensure searching in same category
            # (user, userid, categoryid)
            userTuples = []
            for userid in currUsers:
                cat_id = random.choice(categories.keys())
                userTuples.append((userDict[userid], userid,  cat_id))

            for x in range(20): # Max 20 searches for each user (less if they purchase) 

                # ISO 8601 format compatible with Cassandra
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # For epoch --> time.time()
                
                # For each (user, userid, category), pick a product and action (either 'search' or 'buy')
                for tup in userTuples:
                    product_id = random.choice(categories[tup[2]])
		    ranNum = random.randint(1,20)
                    action = "buy" if ranNum == 1 else "search" # 5% chance of buying
                    userMsg = msg_fmt.format(now, tup[0], tup[1], product_id, tup[2], action)
                    print userMsg # FIXME
                    time.sleep(0.000000001)
                    #self.producer.send_messages('web_activity1', str(ranNum), userMsg) # Where 'web_activity1' is the topic
                    if action == "buy":
                        break


                # Get new users to search this server after a buy
                if action == "buy":
                    break


if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    #partition_key = str(args[2]) # FIXME
    prod = Producer(ip_addr)
    prod.produce_msgs() 


