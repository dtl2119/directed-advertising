/*
Once Cassandra is installed and configured properly across cluster, this is the schema
used to create the keyspace and tables for my application:
KEYSPACE --> 'advertise'
TABLE --> 'usersearches'
*/

CREATE KEYSPACE advertise WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 4};

// May need to add insertion timestamp.  Also, the list might want to be a list of tuples rather than just
// a list of product_ids.  Tuple in order to include timestamp of search
CREATE TABLE usersearches (userid text, categoryid text, searches list<text>, PRIMARY KEY ((userid, categoryid)));

