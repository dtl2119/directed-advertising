/*
Once Cassandra is installed and configured properly across cluster, this is the schema
used to create the keyspace and tables for my application:
KEYSPACE --> 'advertise'
TABLE --> 'usersearches'
TABLE --> 'searches'
TABLE --> 'productcount'
*/

CREATE KEYSPACE advertise WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 4};


// Table for usersearches (and adding a second index, to query by user only)
// Be careful, creating secondary index is not ideal for high-cardinality columns
CREATE TABLE usersearches (user text, userid int, categoryid int, time timestamp, productid int, PRIMARY KEY ((userid, categoryid), time)) WITH CLUSTERING ORDER BY (time DESC);
CREATE INDEX useridx ON advertise.usersearches (userid);

CREATE TABLE advertise.usersearches (
    userid int,
    categoryid int,
    time timestamp,
    productid int,
    user text,
    PRIMARY KEY ((userid, categoryid), time)
) WITH CLUSTERING ORDER BY (time DESC)



// Table for searches
CREATE TABLE searches (user text, userid int, categoryid int, time timestamp, productid int, PRIMARY KEY (userid, time, categoryid)) WITH CLUSTERING ORDER BY (time DESC);



// Table for productcount
CREATE TABLE productcount (categoryid int, productid int, count int, PRIMARY KEY (categoryid, count, productid)) WITH CLUSTERING ORDER BY (count DESC);

CREATE TABLE advertise.productcount (
    categoryid int,
    count int,
    productid int,
    PRIMARY KEY (categoryid, count)
) WITH CLUSTERING ORDER BY (count DESC)
