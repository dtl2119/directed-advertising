# directed-advertising

##### Table of Contents  
- [Clusters](#clusters)  
- [Data Pipeline](#data-pipeline)
- [Spark](#spark)



###Clusters
The data pipeline consists of 2 clusters: `de-ny-drew` and `de-ny-drew2`
Using m4.large AWS EC2 instances, each cluster had a master and 3 workers, for a total of 8 nodes.



###Data Pipeline
    - First Cluster `de-ny-drew`
        - Kafka (Producers and Consumers)
        - Hadoop/HDFS
    - Second Cluster `de-ny-drew2`
        - Spark Engine
        - Cassandra DB


##Spark 
To run pyspark file:
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 --conf spark.cassandra.connection.host="Cassandra DB IPs" <pyspark_file>
