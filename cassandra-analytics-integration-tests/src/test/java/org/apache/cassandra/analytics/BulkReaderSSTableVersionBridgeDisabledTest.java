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
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for SSTable version-based bridge selection in bulk reader (feature DISABLED)
 * This test class has the feature flag disabled via SparkConf override.
 */
class BulkReaderSSTableVersionBridgeDisabledTest extends SharedClusterSparkIntegrationTestBase
{
    static final List<String> DATASET = Arrays.asList("alpha", "beta", "gamma", "delta", "epsilon");
    static final QualifiedName TABLE_DISABLED = new QualifiedName(TEST_KEYSPACE, "test_disabled");

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3);
    }

    @Override
    protected SparkConf getOrCreateSparkConf()
    {
        SparkConf conf = super.getOrCreateSparkConf();
        // Disable SSTable version-based bridge selection for this entire test class
        conf.set("spark.cassandra_analytics.bridge.disable_sstable_version_based", "true");
        return conf;
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);

        // Create table
        createTestTable(TABLE_DISABLED, "CREATE TABLE %s (id int PRIMARY KEY, value text) WITH compression = {'enabled': false};");

        IInstance firstRunningInstance = cluster.getFirstRunningInstance();

        // Insert data for TABLE_DISABLED
        for (int i = 0; i < DATASET.size(); i++)
        {
            String query = String.format("INSERT INTO %s (id, value) VALUES (%d, '%s');", TABLE_DISABLED, i, DATASET.get(i));
            firstRunningInstance.coordinator().execute(query, ConsistencyLevel.ALL);
        }

        // Flush to create SSTables
        cluster.stream().forEach(instance -> {
            instance.nodetool("flush", TEST_KEYSPACE, TABLE_DISABLED.table());
        });
    }

    @Test
    void testBulkReadWithSSTableVersionBasedBridgeDisabled()
    {
        // Read with feature disabled (fallback to cassandra.version)
        Dataset<Row> df = bulkReaderDataFrame(TABLE_DISABLED).load();

        List<Row> rows = df.collectAsList()
                           .stream()
                           .sorted(Comparator.comparing(row -> row.getInt(0)))
                           .collect(Collectors.toList());

        assertThat(rows).hasSize(DATASET.size());
        for (int i = 0; i < DATASET.size(); i++)
        {
            assertThat(rows.get(i).getInt(0)).isEqualTo(i);
            assertThat(rows.get(i).getString(1)).isEqualTo(DATASET.get(i));
        }
    }
}
