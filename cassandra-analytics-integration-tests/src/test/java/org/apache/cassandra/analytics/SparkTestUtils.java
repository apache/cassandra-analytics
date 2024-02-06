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

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.impl.AbstractCluster;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Helper methods for Spark tests
 */
public final class SparkTestUtils
{
    private SparkTestUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    /**
     * Returns a {@link DataFrameReader} with default options for performing a bulk read test, including
     * required parameters.
     *
     * @param sparkConf              the spark configuration to use
     * @param spark                  the spark session to use
     * @param tableName              the qualified name of the table
     * @param sidecarInstancesOption the comma-separated list of sidecar instances
     * @param sidecarPort            the sidecar port
     * @return a {@link DataFrameReader} with default options for performing a bulk read test
     */
    public static DataFrameReader defaultBulkReaderDataFrame(SparkConf sparkConf,
                                                             SparkSession spark,
                                                             QualifiedName tableName,
                                                             String sidecarInstancesOption,
                                                             int sidecarPort)
    {
        SQLContext sql = spark.sqlContext();
        SparkContext sc = spark.sparkContext();

        int coresPerExecutor = sparkConf.getInt("spark.executor.cores", 1);
        int numExecutors = sparkConf.getInt("spark.dynamicAllocation.maxExecutors", sparkConf.getInt("spark.executor.instances", 1));
        int numCores = coresPerExecutor * numExecutors;

        return sql.read().format("org.apache.cassandra.spark.sparksql.CassandraDataSource")
                  .option("sidecar_instances", sidecarInstancesOption)
                  .option("keyspace", tableName.keyspace()) // unquoted
                  .option("table", tableName.table()) // unquoted
                  .option("DC", "datacenter1")
                  .option("snapshotName", UUID.randomUUID().toString())
                  .option("createSnapshot", "true")
                  // Shutdown hooks are called after the job ends, and in the case of integration tests
                  // the sidecar is already shut down before this. Since the cluster will be torn
                  // down anyway, the integration job skips clearing snapshots.
                  .option("clearSnapshotStrategy", "noop")
                  .option("defaultParallelism", sc.defaultParallelism())
                  .option("numCores", numCores)
                  .option("sizing", "default")
                  .option("sidecar_port", sidecarPort);
    }

    /**
     * Returns a {@link DataFrameWriter<Row>} with default options for performing a bulk write test, including
     * required parameters.
     *
     * @param df                     the source data frame
     * @param tableName              the qualified name of the table
     * @param sidecarInstancesOption the comma-separated list of sidecar instances
     * @param sidecarPort            the sidecar port
     * @return a {@link DataFrameWriter<Row>} with default options for performing a bulk write test
     */
    public static DataFrameWriter<Row> defaultBulkWriterDataFrameWriter(Dataset<Row> df,
                                                                        QualifiedName tableName,
                                                                        String sidecarInstancesOption,
                                                                        int sidecarPort)
    {
        return df.write()
                 .format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
                 .option("sidecar_instances", sidecarInstancesOption)
                 .option("keyspace", tableName.keyspace())
                 .option("table", tableName.table())
                 .option("local_dc", "datacenter1")
                 .option("bulk_writer_cl", "LOCAL_QUORUM")
                 .option("number_splits", "-1")
                 .option("sidecar_port", sidecarPort)
                 .mode("append");
    }

    public static SparkConf defaultSparkConf()
    {
        SparkConf sparkConf = new SparkConf()
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
        return sparkConf;
    }

    public static void validateWrites(AbstractCluster<? extends IInstance> cluster, QualifiedName tableName, Dataset<Row> df)
    {
        // build a set of entries read from Cassandra into a set
        Set<String> actualEntries = Arrays.stream(cluster.coordinator(1)
                                                         .execute(String.format("SELECT * FROM %s;", tableName), ConsistencyLevel.LOCAL_QUORUM))
                                          .map((Object[] columns) -> String.format("%s:%s:%s",
                                                                                   columns[0],
                                                                                   columns[1],
                                                                                   columns[2]))
                                          .collect(Collectors.toSet());

        // Number of entries in Cassandra must match the original datasource
        assertThat(actualEntries.size()).isEqualTo(df.count());

        // remove from actual entries to make sure that the data read is the same as the data written
        df.collectAsList()
          .forEach(row -> {
              String key = String.format("%d:%s:%d",
                                         row.getInt(0),
                                         row.getString(1),
                                         row.getInt(2));
              assertThat(actualEntries.remove(key)).as(key + " is expected to exist in the actual entries")
                                                   .isTrue();
          });

        // If this fails, it means there was more data in the database than we expected
        assertThat(actualEntries).as("All entries are expected to be read from database")
                                 .isEmpty();
    }
}
