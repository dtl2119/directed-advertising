#!/usr/bin/python

from cassandra.cluster import Cluster

cluster = Cluster()
cluster = Cluster(['172.31.0.133']) # Only need 1 private ip, it will auto discover remaining nodes in cluster
session = cluster.connect()
session = cluster.connect('advertise') # advertise is the keyspace
rows = session.execute('SELECT * FROM usersearches')

for r in rows:
    print r

# Row(userid=u'11111111', categoryid=u'11111111', searches=[u'11111111', u'11111111', u'11111111'])


# How to insert
session.execute(
    """
    INSERT INTO usersearches (userid, categoryid, searches)
    VALUES (%s, %s, %s)
    """,
    ("222222", "222222", ["222222", "222222", "222222"])
)
