#!/usr/bin/python


#with open("userids.txt", 'r') as ids:
#userids = ids.readlines()
with open("usernames.txt", 'r') as names:
    with open("users_dict.py", 'w') as out:
        usernames = names.read().splitlines()
        #userids = [x for x in range(1, len(usernames))]
        i = 1
        while i <= 50000:
            line = "%s: \"%s\"\n" % (i, usernames[i-1])
            out.write(line)
            i += 1
