/*
Once Cassandra is installed and configured properly across cluster, this is the schema
used to create the keyspace and tables for my application:
KEYSPACE --> 'advertise'
TABLE --> 'usersearches'
*/

CREATE KEYSPACE advertise WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 4};

CREATE TABLE usersearches (userid text, categoryid text, searches list<text>, PRIMARY KEY ((userid, categoryid)));

