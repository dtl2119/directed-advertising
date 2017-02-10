# directed-advertising
---

##### Table of Contents  
[Clusters](#clusters)  
[Data Pipeline](#data-pipeline)
[Spark](#spark)




###Clusters
    - First Cluster `de-ny-drew`
        - Kafka (Producers and Consumers)
        - Hadoop/HDFS
    - Second Cluster `de-ny-drew2`
        - Spark Engine
        - Cassandra DB

###Data Pipeline



##Spark 
To run pyspark file:
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 --conf spark.cassandra.connection.host="Cassandra DB IPs" <pyspark_file>
