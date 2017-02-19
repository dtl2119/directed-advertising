# directed-advertising

##### Table of Contents  
- [Tools and Frameworks](#frameworks-and-versions)

- [Clusters](#clusters)  

- [Data Pipeline](#data-pipeline)

- [Spark](#spark)

---

###Tools and Frameworks
Kafka: 0.10.1 (Scala 2.10)
kafka-python: 1.3.2 (python 2.7)
Hadoop 2.7.2

Spark: 2.1.0
Cassandra: 3.9
Flask: 0.12

###Clusters
The data pipeline consists of 2 clusters: `de-ny-drew` and `de-ny-drew2`
Using AWS EC2 instances, each cluster had a master and 3 workers, for a total of 8 nodes.

The EC2 machines were instance-type m4.large
Running Ubuntu 14.04.2 LTS (trusty)



###Data Pipeline
    - First Cluster `de-ny-drew`
        - Kafka (Producers and Consumers)
        - Hadoop/HDFS
    - Second Cluster `de-ny-drew2`
        - Spark Engine
        - Cassandra DB


###Spark 
To run the spark programs, cd into the `spark` directory. There are two files: `batch_aDirect.py` and `stream_aDirect.py`.

batch:
`spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 --conf spark.cassandra.connection.host="Cassandra DB IPs" batch_aDirect.py`

streaming:
`spark-submit --master <master_network_socket> --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.1.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 stream_aDirect.py`
