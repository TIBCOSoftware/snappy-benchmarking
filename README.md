# Benchmark

## Run This Benchmark Yourself
This benchmark can be easily replicated using the machine configuration provided in system configuration.

## Hosted data sets
To allow this benchmark to be easily reproduced, we have copied airline dataset to AWS S3. It is available publicly at s3a://AKIAIA4A774XGNFXE2LA:7tufh8T56e3smwQJOZdkRI46xuTZyfwLwakI36kt@zeppelindemo/1995_2015AirlineData
AirportCode dataset can be found at below location on [git](https://github.com/SnappyDataInc/snappydata/tree/master/examples/quickstart/data/airportcodeParquetData)

## Launching and Loading Clusters
1. Creating the Spark cluster and running a Spark app to get the numbers
	Start the Spark cluster using the default configuration and required parameters mentioned in cluster Cluster Setup and Product Details section.

	Below is the spark-submit command with the options used in this benchmark for getting Spark numbers:

  ```./bin/spark-submit --master spark://<master-host>:<master-port> --class LoadAndQuerySparkApp --jars <path to jar location>/spark-hive_2.11-1.2.0.jar --driver-memory 10G --executor-memory 20G <app- jar>```

2. Creating SnappyData + Spark cluster and running the SnappyData Spark application and SnappyData job to get the numbers for Embedded and Smart Connector mode
	Start the SnappyData + Spark cluster using the default configurations and required parameters mentioned in cluster Cluster Setup and Product Details section.

	Below is the spark-submit command with the options used in this benchmark for getting SnappyData Smart Connector Mode numbers:
  ```./bin/spark-submit --master spark://<master-host>:<master-port> --class LoadAndQuerySnappySparkApp  --driver-memory 10G --executor-memory 20G --conf spark.memory-manager=org.apache.spark.memory.SnappyUnifiedMemoryManager --conf spark.snappydata.store.memory-size=4g --jars <snappydata-core_2.11-1.0.0.jar path> <app-jar> <SnappyData Locator host>:<SnappyData locator port>```

	Below is the snappy-job submit command showing the options used in this benchmark for getting SnappyData Embedded Mode numbers:

  ```./bin/snappy-job.sh submit --class LoadAndQueryPerfSnappyJob --app-name myapp --app-jar <app-jar-path>```

3. Creating Alluxio + Spark cluster and running Spark app to get the numbers
	Start the Alluxio + Spark cluster using the default configurations and required parameters mentioned in cluster Configuration section.

	Below is the spark-submit command showing the options used in this benchmark for getting Alluxio Connector Mode numbers:

  ```./bin/spark-submit --master spark://<master-host>:<master-port> --class LoadAndQueryPerfAlluxioSparkApp --jars <path to jar location>/alluxio-1.6.1-spark-client.jar --driver-memory 10G --executor-memory 20G <app-jar-path>```


4. Creating Kudu + Spark cluster and running Spark app to get the numbers
	Start the Kudu + Spark cluster using the default configurations and required parameters mentioned in cluster Cluster Setup and Product Details section.

	Below is the spark-submit command with the options used in this benchmark for getting Kudu Connector Mode numbers:

  ```./bin/spark-submit --master spark://<master-host>:<master-port> --class LoadAndQueryPerfKuduSparkApp --jars <path to jar location>/kudu-client-1.5.0.jar,<path to jar location>/kudu-spark2_2.11-1.5.0.jar --driver-memory 10G --executor-memory 20G <app-jar-path>```


5. Creating Cassandra + Spark cluster and running Spark app to get the numbers
    Start the Cassandra + Spark cluster using the default configurations and required parameters mentioned in cluster Cluster Setup and Product Details section.

	Below is the spark-submit command showing the options used in this benchmark for getting Cassandra Connector Mode numbers:

  ```./bin/spark-submit --master spark://<master-host>:<master-port> --class LoadAndQueryPerfCassandraSparkApp --jars <path to jar location>/spark-hive_2.11-1.2.0.jar --conf spark.cassandra.connection.host=127.0.0.1 --conf spark.local.dir=<path to temp dir>  --packages datastax:spark-cassandra-connector:2.0.5-s_2.11  --driver-memory 10G --executor-memory 20G <app-jar-path>```


## System Configuration

| Operating System | Configuration |
|--------|--------|
|Ubuntu 16.04.1 LTS|GNU/Linux 4.4.0-98-generic x86_64 </p>Azure VM with:</p>- 16 Cores</p>- 112 GB RAM|
