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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import com.vdurmont.semver4j.Semver;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Tests bulk write and read from a table with mutation tracking enabled
 */
class AnalyticsForTrackedTableTest extends SharedClusterSparkIntegrationTestBase
{
    private static final String TRACKED_KEYSPACE = "spark_test_tracked";
    private static final QualifiedName TRACKED_TABLE = new QualifiedName(TRACKED_KEYSPACE, "tracked_bulk_write");
    private static final long REPLICATION_TIMEOUT_SECONDS = 30;

    @Test
    void bulkWriteReadFromTrackedTable()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> dfWrite = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);

        bulkWriterDataFrameWriter(dfWrite, TRACKED_TABLE).save();

        awaitEveryReplicaHoldsAllRows(dfWrite);

        Dataset<Row> read = bulkReaderDataFrame(TRACKED_TABLE).load();
        List<Row> rows = read.collectAsList().stream()
                             .sorted(Comparator.comparing(row -> row.getInt(0)))
                             .collect(Collectors.toList());
        assertThat(rows.size()).isEqualTo(ROW_COUNT);
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        Semver clusterVersion = TestUtils.getDTestClusterVersion().withClearedSuffixAndBuild();
        assumeThat(clusterVersion.isGreaterThanOrEqualTo(new Semver("6.0", Semver.SemverType.LOOSE)))
        .describedAs("Mutation tracking requires Cassandra 6.0 or later")
        .isTrue();
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

    private void createTrackedTestKeyspace(String keyspace, Map<String, Integer> rf)
    {
        cluster.schemaChangeIgnoringStoppedInstances("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                                                     + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', "
                                                     + generateRfString(rf) + " }"
                                                     + " AND replication_type = 'tracked';");
    }

    private void awaitEveryReplicaHoldsAllRows(Dataset<Row> dfWrite)
    {
        Set<String> expectedRows = dfWrite.collectAsList().stream()
                                          .map(row -> row.getInt(0) + ":" + row.getString(1) + ":" + row.getInt(2))
                                          .collect(Collectors.toSet());
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(REPLICATION_TIMEOUT_SECONDS);
        Map<Integer, Set<String>> rowsByNode = new TreeMap<>();
        while (true)
        {
            rowsByNode.clear();
            cluster.stream().forEach(instance -> rowsByNode.put(instance.config().num(), localRows(instance)));
            if (rowsByNode.values().stream().allMatch(expectedRows::equals) || System.nanoTime() > deadlineNanos)
            {
                break;
            }
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        }

        rowsByNode.forEach((nodeNumber, rows) ->
                           assertThat(rows)
                           .describedAs("node %s did not receive every row from the bulk write coordinator within"
                                        + "%s seconds", nodeNumber, REPLICATION_TIMEOUT_SECONDS)
                           .isEqualTo(expectedRows));
    }

    private Set<String> localRows(IInstance instance)
    {
        SimpleQueryResult result = instance.executeInternalWithResult("SELECT id, course, marks FROM " + TRACKED_TABLE);
        Set<String> rows = new HashSet<>();
        while (result.hasNext())
        {
            org.apache.cassandra.distributed.api.Row row = result.next();
            rows.add(row.getInteger("id") + ":" + row.getString("course") + ":" + row.getInteger("marks"));
        }
        return rows;
    }
}
