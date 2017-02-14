#!/usr/bin/python

from badwords import badList

userList = []

with open("/Users/drewlimm/insight/directed-advertising/usernames/10-million-combos.txt") as original:
    o_lines = original.readlines()
    for line in o_lines:
        try:
            username = line.split()[1] # get second value, username-like
        except IndexError as e:
            continue

        username = username.lower()

        if not username.isalnum():
            continue
        
        if username == len(username) * username[0]:
            continue

        # Remove usernames with
        #if username == len(username) * username[0]:
        #    continue
       

        if username in badList:
            continue

        # Skip if starts with number
        if username[0].isdigit():
            continue
            
        # Remove usernames with bad words
        for word in badList:
            if word in username:
                username = "remove"
                continue

        userList.append(username)


with open("/Users/drewlimm/insight/directed-advertising/usernames/shorter_users.txt", 'a') as users:
    userSet = set(userList)

    for user in userSet:
        users.write(user + "\n")

