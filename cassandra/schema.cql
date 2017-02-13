/*
Once Cassandra is installed and configured properly across cluster, this is the schema
used to create the keyspace and tables for my application:
KEYSPACE --> 'advertise'
TABLE --> 'usersearches'
*/

CREATE KEYSPACE advertise WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 4};

// May need to add insertion timestamp.  Also, the list might want to be a list of tuples rather than just
// a list of product_ids.  Tuple in order to include timestamp of search
// Tih has userid, categoryid, and searches in different colors
// PRIMARY KEY (userid, categoryid)
// WITH CLUSTERING ORDER BY (categoryid ASC)
CREATE TABLE usersearches (userid text, categoryid text, searches set<text>, PRIMARY KEY (userid, categoryid));



// This has userid and category id in red
// PRIMARY KEY ((userid, categoryid)) WITH NO CLUSTERING ORDER
CREATE TABLE usersearches (userid text, categoryid text, searches set<text>, PRIMARY KEY ((userid, categoryid)));


// Table for deletions
CREATE TABLE deletions (userid text, categoryid text, productid text, PRIMARY KEY (userid, categoryid));


// Table for buy counts
CREATE TABLE userbuys (productid text, count int, PRIMARY KEY (productid));
