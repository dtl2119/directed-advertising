/*
Once Cassandra is installed and configured properly across cluster, this is the schema
used to create the keyspace and tables for my application:
KEYSPACE --> 'advertise'
TABLE --> 'usersearches'
*/

CREATE KEYSPACE advertise WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 4};

// PRIMARY KEY (userid, categoryid)
// WITH CLUSTERING ORDER BY (categoryid ASC)
CREATE TABLE usersearches (userid text, categoryid text, searches set<text>, PRIMARY KEY (userid, categoryid));



// PRIMARY KEY ((userid, categoryid)) WITH NO CLUSTERING ORDER
CREATE TABLE usersearches (userid text, categoryid text, searches set<text>, PRIMARY KEY ((userid, categoryid)));


// Table for deletions
CREATE TABLE deletions (userid text, categoryid text, productid text, PRIMARY KEY (userid, categoryid));


// Table for buy counts
CREATE TABLE userbuys (productid text, count int, PRIMARY KEY (productid));


// New table for usersearches
CREATE TABLE usersearches (userid text, categoryid text, time timestamp, productid text, PRIMARY KEY ((userid, categoryid), time)) WITH CLUSTERING ORDER BY (time DESC);
