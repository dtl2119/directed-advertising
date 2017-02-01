#!/usr/bin/python

import argparse
import time
from datetime import datetime
import random
from random import randint
import sys
from faker import Faker

batch_file_path= "/home/ubuntu/directed-advertising/small_batch_file.txt"

# CHANGE: append larger "category id" to beginning
tvs = [int(str(11)+ str(tv)) for tv in range(0, (5+1)*10, 10)[1:]] # tv ids: 5 tvs, multiple of 10, append 1 to front of number
cables = [int(str(22)+ str(cable)) for cable in range(0, (10+1)*5, 5)[1:]] # cables ids: 10 cables, multiple of 5, append 2 to front of number
cameras = [int(str(33)+ str(camera)) for camera in range(0, (15+1)*3, 3)[1:]] # cables ids: 15 cameras, multiple of 3, append 3 to front of number
computers = [int(str(44)+ str(comp)) for comp in range(0, (20+1)*4, 4)[1:]] # cables ids: 20 computers, multiple of 4, append 4 to front of number

categories = {1:tvs, 2: cables, 3:cameras, 4:computers}

parser = argparse.ArgumentParser(__file__, description="Generate user search and purchase history")
parser.add_argument("--num", "-n", dest='num_lines', help="Number of lines to generate (0 for infinite)", type=int, default=1)

args = parser.parse_args()
logLines = args.num_lines

# For each producer/process, change range
# Ex: process1: userids = 1 to 1000
#     process2: userids = 1001 to 2000
#     process3: userids = 2001 to 3000
num_user = 1000 # Number of unique users
userSet = random.sample(range(1, num_user+1), num_user)

with open(batch_file_path, "w") as batchFile:
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
            
            # Compute Action (either search or buy)
            # 5% Chance of buying
            user1Action = "buy" if randint(1,20) == 1 else "search"
            user2Action = "buy" if randint(1,20) == 1 else "search"

            # WRITE SEARCH
            batchFile.write("%s, %s, %s, %s, %s\n" % (now, user1, user1ProductId, 1, user1Action))
            batchFile.write("%s, %s, %s, %s, %s\n" % (now, user2, user2ProductId, 2, user2Action))
            logLines -= 2

            
            if logLines <= 0:
                break

            
        flag = False if logLines <= 0 else True
