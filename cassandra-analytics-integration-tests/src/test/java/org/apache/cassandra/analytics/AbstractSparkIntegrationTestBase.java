/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.analytics;

import java.net.UnknownHostException;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.sidecar.testing.AbstractIntegrationTestBase;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Extends functionality from {@link AbstractIntegrationTestBase} and provides additional functionality for running
 * Spark integration tests.
 */
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
public abstract class AbstractSparkIntegrationTestBase extends AbstractIntegrationTestBase
{
    protected SparkConf sparkConf;
    protected SparkSession sparkSession;

    /**
     * A preconfigured {@link DataFrameReader} with pre-populated required options that can be overridden
     * with additional options for every specific test.
     *
     * @param tableName the qualified name for the Cassandra table
     * @return a {@link DataFrameReader} for Cassandra bulk reads
     */
    protected DataFrameReader bulkReaderDataFrame(QualifiedName tableName)
    {
        SparkConf sparkConf = getOrCreateSparkConf();
        SparkSession spark = getOrCreateSparkSession();
        SQLContext sql = spark.sqlContext();
        SparkContext sc = spark.sparkContext();

        int coresPerExecutor = sparkConf.getInt("spark.executor.cores", 1);
        int numExecutors = sparkConf.getInt("spark.dynamicAllocation.maxExecutors",
                                            sparkConf.getInt("spark.executor.instances", 1));
        int numCores = coresPerExecutor * numExecutors;

        return sql.read()
                  .format("org.apache.cassandra.spark.sparksql.CassandraDataSource")
                  .option("sidecar_instances", sidecarInstancesOption())
                  .option("keyspace", tableName.keyspace()) // unquoted
                  .option("table", tableName.table()) // unquoted
                  .option("DC", "datacenter1")
                  .option("snapshotName", UUID.randomUUID().toString())
                  .option("createSnapshot", "true")
                  // Shutdown hooks are called after the job ends, and in the case of integration tests
                  // the sidecar is already shut down before this. Since the cluster will be torn
                  // down anyway, the integration job skips clearing snapshots.
                  .option("clearSnapshot", "false")
                  .option("defaultParallelism", sc.defaultParallelism())
                  .option("numCores", numCores)
                  .option("sizing", "default")
                  .option("sidecar_port", server.actualPort());
    }

    /**
     * A preconfigured {@link DataFrameWriter} with pre-populated required options that can be overridden
     * with additional options for every specific test.
     *
     * @param df        the source dataframe to write
     * @param tableName the qualified name for the Cassandra table
     * @return a {@link DataFrameWriter} for Cassandra bulk writes
     */
    protected DataFrameWriter<Row> bulkWriterDataFrameWriter(Dataset<Row> df, QualifiedName tableName)
    {
        return df.write()
                 .format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
                 .option("sidecar_instances", sidecarInstancesOption())
                 .option("keyspace", tableName.keyspace())
                 .option("table", tableName.table())
                 .option("local_dc", "datacenter1")
                 .option("bulk_writer_cl", "LOCAL_QUORUM")
                 .option("number_splits", "-1")
                 .option("sidecar_port", server.actualPort())
                 .mode("append");
    }

    /**
     * @return a comma-separated string with a list of all the hosts in the in-jvm dtest cluster
     */
    protected String sidecarInstancesOption()
    {
        return IntStream.rangeClosed(1, cluster.size())
                        .mapToObj(i -> {
                            String ipAddress = JMXUtil.getJmxHost(cluster.get(i).config());
                            try
                            {
                                return dnsResolver.reverseResolve(ipAddress);
                            }
                            catch (UnknownHostException e)
                            {
                                return ipAddress;
                            }
                        })
                        .collect(Collectors.joining(","));
    }

    protected SparkConf getOrCreateSparkConf()
    {
        if (sparkConf == null)
        {
            sparkConf = new SparkConf()
                        .setAppName("Integration test Spark Cassandra Bulk Analytics Job")
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        // Spark is not case-sensitive by default, but we want to make it case-sensitive for
                        // the quoted identifiers tests where we test mixed case
                        .set("spark.sql.caseSensitive", "True")
                        .set("spark.master", "local[8,4]")
                        .set("spark.cassandra_analytics.sidecar.request.retries", "5")
                        .set("spark.cassandra_analytics.sidecar.request.retries.delay.milliseconds", "500")
                        .set("spark.cassandra_analytics.sidecar.request.retries.max.delay.milliseconds", "500");
            BulkSparkConf.setupSparkConf(sparkConf, true);
            KryoRegister.setup(sparkConf);
        }
        return sparkConf;
    }

    protected SparkSession getOrCreateSparkSession()
    {
        if (sparkSession == null)
        {
            sparkSession = SparkSession
                           .builder()
                           .config(getOrCreateSparkConf())
                           .getOrCreate();
        }
        return sparkSession;
    }
}
