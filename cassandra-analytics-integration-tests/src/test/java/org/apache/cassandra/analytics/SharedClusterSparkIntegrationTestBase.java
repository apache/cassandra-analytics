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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterIntegrationTestBase;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.analytics.SparkTestUtils.defaultBulkReaderDataFrame;
import static org.apache.cassandra.analytics.SparkTestUtils.defaultBulkWriterDataFrameWriter;
import static org.apache.cassandra.analytics.SparkTestUtils.defaultSparkConf;

/**
 * Extends functionality from {@link SharedClusterIntegrationTestBase} and provides additional functionality for running
 * Spark integration tests.
 */
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
public abstract class SharedClusterSparkIntegrationTestBase extends SharedClusterIntegrationTestBase
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
        return defaultBulkReaderDataFrame(getOrCreateSparkConf(),
                                          getOrCreateSparkSession(),
                                          tableName,
                                          sidecarInstancesOption(),
                                          server.actualPort());
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
        return defaultBulkWriterDataFrameWriter(df, tableName, sidecarInstancesOption(), server.actualPort());
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
            sparkConf = defaultSparkConf();
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
