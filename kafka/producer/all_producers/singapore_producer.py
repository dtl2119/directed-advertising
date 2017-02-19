#!/usr/bin/python

import os, sys
sys.path.append("%s/directed-advertising/gitignored" % (os.environ['HOME']))
from cluster_ips import first
from cluster_ips import kafka
import users

import random
import time
from datetime import datetime
from kafka import KafkaProducer


# Total Products = itemsPerCat * 10 = 1,000,000
itemsPerCat = 100000

# Product id lists: prepend categoryid*3 (i.e. '1'*3 == '111) so we can 
# easily determine which category a certain productid belongs to
tvs = ['111' + str(x) for x in range(itemsPerCat)]
cables = ['222' + str(x) for x in range(itemsPerCat)]
cameras = ['333' + str(x) for x in range(itemsPerCat)]
phones = ['444' + str(x) for x in range(itemsPerCat)]
computers = ['555' + str(x) for x in range(itemsPerCat)]
memory = ['666' + str(x) for x in range(itemsPerCat)]
monitors = ['777' + str(x) for x in range(itemsPerCat)]
audio = ['888' + str(x) for x in range(itemsPerCat)]
chargers = ['999'+ str(x) for x in range(itemsPerCat)]
misc = ['100'+ str(x) for x in range(itemsPerCat)]

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

# users.<country> is a dictionary from users.py (gitignored)
# k:v =  userid: username (i.e. 49723: "craig198")
userDict = users.singapore

# Format of messages to be sent: csv
msg_fmt = "{},{},{},{},{},{}"
class Producer(object):

    def produce_msgs(self, bootstrap_servers):
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        while (True):
            time.sleep(0.01)

            # Pick 5 random user ids
            currUsers = random.sample(userDict.keys(), 5)
           
            # Tuple for users, ensure searching in same category
            # (user, userid, categoryid)
            userTuples = []
            for userid in currUsers:
                categoryid = random.choice(categories.keys())
                userTuples.append((userDict[userid], userid, categoryid))

            # Max 100 searches for each user (less if they purchase) 
            for x in range(100): # Max 100 searches for each user (less if they purchase) 

                # ISO 8601 format compatible with Cassandra
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # For epoch --> time.time()
                
                # For each (user, userid, category), pick a product and action (either 'search' or 'buy')
                for tup in userTuples:
                    product_id = random.choice(categories[tup[2]])
		    ranNum = random.randint(1,100)
                    action = "buy" if ranNum == 1 else "search" # 1% chance of buying
                    userMsg = msg_fmt.format(now, tup[0], tup[1], product_id, tup[2], action)
                    producer.send('web_activity1', userMsg)
                    if action == "buy":
                        break

                # Get new users to search this server after a buy
                if action == "buy":
                    break

if __name__ == "__main__":
    producer = Producer()
    socket_format = "{}:{}"
    bootstrap_servers = [socket_format.format(v, kafka['port']) for v in first.values()]
    producer.produce_msgs(bootstrap_servers)
        


