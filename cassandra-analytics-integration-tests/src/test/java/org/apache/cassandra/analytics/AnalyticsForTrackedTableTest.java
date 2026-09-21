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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests bulk write and read from a table with mutation tracking enabled
 */
class AnalyticsForTrackedTableTest extends SharedClusterSparkIntegrationTestBase
{
    static final String TRACKED_KEYSPACE = "spark_test_tracked";
    static final QualifiedName TRACKED_TABLE = new QualifiedName(TRACKED_KEYSPACE, "tracked_bulk_write");

    @Test
    void bulkWriteReadFromTrackedTable()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> dfWrite = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        bulkWriterDataFrameWriter(dfWrite, TRACKED_TABLE).save();

        // Read the data back using Bulk Reader and validate that written and read dataframes are the same
        Dataset<Row> read = bulkReaderDataFrame(TRACKED_TABLE).load();
        List<Row> rows = read.collectAsList().stream()
                             .sorted(Comparator.comparing(row -> row.getInt(0)))
                             .collect(Collectors.toList());
        // TODO: uncomment this check once SSTable import for tracked tables is fixed in Cassandra.
        //  Currently wrap around shards are silently skipped during SSTable import causing data loss
//        assertThat(rows.size()).isEqualTo(ROW_COUNT);
        assertThat(rows.size()).isGreaterThan(0);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        ClusterBuilderConfiguration conf = super.testClusterConfiguration()
                                                .nodesPerDc(3)
                                                // NETWORK is needed for instances to bind their storage port and accept
                                                // SSTable transfers
                                                .requestFeature(Feature.NETWORK);
        Map<String, Object> instanceConfig = new HashMap<>();
        if (conf.additionalInstanceConfig != null)
        {
            instanceConfig.putAll(conf.additionalInstanceConfig);
        }
        instanceConfig.put("mutation_tracking.enabled", true);
        instanceConfig.put("repair_request_timeout", "600000ms");
        return conf.additionalInstanceConfig(instanceConfig);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTrackedTestKeyspace(TRACKED_KEYSPACE, DC1_RF3);
        createTestTable(TRACKED_TABLE, CREATE_TEST_TABLE_STATEMENT);
    }

    void createTrackedTestKeyspace(String keyspace, Map<String, Integer> rf)
    {
        cluster.schemaChangeIgnoringStoppedInstances("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                                                     + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', "
                                                     + generateRfString(rf) + " }"
                                                     + " AND replication_type = 'tracked';");
    }
}
