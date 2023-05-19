# Cassandra Analytics 

## Cassandra Spark Bulk Reader

The open-source repository for the Cassandra Spark Bulk Reader. This library allows integration between Cassandra and Spark job, allowing users to run arbitrary Spark jobs against a Cassandra cluster securely and consistently. 

This project contains the necessary open-source implementations to connect to a Cassandra cluster and read the data into Spark.

For example usage, see the example repository; sample steps:

```scala
import org.apache.cassandra.spark.sparksql.CassandraDataSource
import org.apache.spark.sql.SparkSession

val sparkSession = SparkSession.builder.getOrCreate()
val df = sparkSession.read.format("org.apache.cassandra.spark.sparksql.CassandraDataSource")
                          .option("sidecar_instances", "localhost,localhost2,localhost3")
                          .option("keyspace", "sbr_tests")
                          .option("table", "basic_test")
                          .option("DC", "datacenter1")
                          .option("createSnapshot", true)
                          .option("numCores", 4)
                          .load()
```
   
## Cassandra Spark Bulk Writer

The Cassandra Spark Bulk Writer allows for high-speed data ingest to Cassandra clusters running Cassandra 3.0 and 4.0.

If you are a consumer of the Cassandra Spark Bulk Writer, please see our end-user documentation: usage instructions, FAQs, troubleshooting guides, and release notes.

Developers interested in contributing to the SBW, please see the [DEV-README](DEV-README.md).

## Getting Started

For example usage, see the [example repository](cassandra-analytics-core-example/README.md). This example covers both
setting up Cassandra 4.0, Apache Sidecar, and running a Spark Bulk Reader and Spark Bulk Writer job.
