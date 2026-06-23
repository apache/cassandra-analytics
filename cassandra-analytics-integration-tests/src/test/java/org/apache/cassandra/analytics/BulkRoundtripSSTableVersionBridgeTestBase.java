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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.vdurmont.semver4j.Semver;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Shared roundtrip integration tests for SSTable version-based bridge selection. The same test logic runs
 * for both the feature-enabled and feature-disabled cases.
 *
 * <p>Each test exercises a full write/read cycle and asserts the on-disk SSTable files use the format and
 * version expected for the Cassandra version under test.</p>
 * <ul>
 *     <li>{@link #testBulkWriteReadRoundtrip()} writes via the bulk writer, then reads back via the bulk reader.</li>
 *     <li>{@link #testBulkReadAcrossMultipleSSTables()} writes via CQL across several flushed SSTables, then bulk reads.</li>
 *     <li>{@link #testBulkReadFromSnapshot()} writes via CQL, snapshots, then bulk reads from the snapshot.</li>
 * </ul>
 */
abstract class BulkRoundtripSSTableVersionBridgeTestBase extends SharedClusterSparkIntegrationTestBase
{
    static final List<String> DATASET = Arrays.asList("alpha", "beta", "gamma", "delta", "epsilon");
    static final QualifiedName TABLE_ROUNDTRIP = new QualifiedName(TEST_KEYSPACE, "test_roundtrip");
    static final QualifiedName TABLE_MULTIPLE = new QualifiedName(TEST_KEYSPACE, "test_multiple");
    static final QualifiedName TABLE_SNAPSHOT = new QualifiedName(TEST_KEYSPACE, "test_snapshot");

    static final StructType SCHEMA = DataTypes.createStructType(new StructField[]{
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("value", DataTypes.StringType, false)
    });

    /**
     * @return whether SSTable version-based bridge selection should be enabled for this test class
     */
    protected abstract boolean sstableVersionBridgeEnabled();

    /**
     * @return the SSTable format the analytics bulk writer should produce ({@code big} by default;
     *         overridden to {@code bti} by the BTI variant).
     */
    protected String sstableFormat()
    {
        return "big";
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        ClusterBuilderConfiguration conf = super.testClusterConfiguration()
                                                .nodesPerDc(3);
        // Preserve the base instance config (e.g. storage_compatibility_mode) and pin the node SSTable format.
        Map<String, Object> instanceConfig = new HashMap<>();
        if (conf.additionalInstanceConfig != null)
        {
            instanceConfig.putAll(conf.additionalInstanceConfig);
        }
        instanceConfig.put("sstable.selected_format", sstableFormat());
        return conf.additionalInstanceConfig(instanceConfig);
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        // Set before CassandraVersion class initialization (which reads this property once, statically) so the bulk
        // writer generates SSTables in the configured format. This runs at the start of cluster provisioning, before
        // any analytics bridge usage, and relies on forkEvery = 1 (the integration-test default) for a fresh JVM per
        // test class.
        System.setProperty("cassandra.analytics.bridges.sstable_format", sstableFormat());
        if ("bti".equals(sstableFormat()))
        {
            // BTI (bti-da) is a Cassandra 5.0+ format; skip on older versions.
            Semver version = new Semver(testVersion.version(), Semver.SemverType.LOOSE);
            assumeTrue(version.isGreaterThanOrEqualTo(new Semver("5.0", Semver.SemverType.LOOSE)),
                       "BTI format (bti-da) requires Cassandra 5.0+, but test version is " + testVersion.version());
        }
    }

    @Override
    protected SparkConf getOrCreateSparkConf()
    {
        SparkConf conf = super.getOrCreateSparkConf();
        // The feature is toggled via the "disable" flag, so it is the inverse of the enabled state.
        conf.set("spark.cassandra_analytics.bridge.disable_sstable_version_based",
                 String.valueOf(!sstableVersionBridgeEnabled()));
        return conf;
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        createTestTable(TABLE_ROUNDTRIP, "CREATE TABLE %s (id int PRIMARY KEY, value text) WITH compression = {'enabled': false};");
        createTestTable(TABLE_MULTIPLE, "CREATE TABLE %s (id int PRIMARY KEY, value text) WITH compression = {'enabled': false};");
        createTestTable(TABLE_SNAPSHOT, "CREATE TABLE %s (id int PRIMARY KEY, value text) WITH compression = {'enabled': false};");
    }

    @Test
    void testBulkWriteReadRoundtrip()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<Row> data = Arrays.asList(
            RowFactory.create(0, "a"),
            RowFactory.create(1, "b"),
            RowFactory.create(2, "c"),
            RowFactory.create(3, "d"),
            RowFactory.create(4, "e")
        );
        Dataset<Row> dfWrite = spark.createDataFrame(data, SCHEMA);

        // Write through the bulk writer
        bulkWriterDataFrameWriter(dfWrite, TABLE_ROUNDTRIP).save();
        flushKeyspace();

        // The bulk writer should have produced SSTables in the expected format/version for this Cassandra version
        assertExpectedSSTableFormat(TABLE_ROUNDTRIP);

        // Read the data back through the bulk reader and confirm the roundtrip
        Dataset<Row> dfRead = bulkReaderDataFrame(TABLE_ROUNDTRIP).load();
        checkSmallDataFrameEquality(dfWrite, dfRead);
    }

    @Test
    void testBulkReadAcrossMultipleSSTables()
    {
        IInstance firstRunningInstance = cluster.getFirstRunningInstance();

        // Insert in 3 batches, flushing between each to create multiple SSTables
        for (int batch = 0; batch < 3; batch++)
        {
            for (int i = batch * 10; i < (batch + 1) * 10; i++)
            {
                String query = String.format("INSERT INTO %s (id, value) VALUES (%d, 'value_%d');", TABLE_MULTIPLE, i, i);
                firstRunningInstance.coordinator().execute(query, ConsistencyLevel.ALL);
            }
            cluster.stream().forEach(instance -> instance.nodetool("flush", TEST_KEYSPACE, TABLE_MULTIPLE.table()));
        }

        assertExpectedSSTableFormat(TABLE_MULTIPLE);

        Dataset<Row> df = bulkReaderDataFrame(TABLE_MULTIPLE).load();
        List<Integer> ids = df.collectAsList()
                              .stream()
                              .map(row -> row.getInt(0))
                              .sorted()
                              .collect(Collectors.toList());
        assertThat(ids).hasSize(30); // 3 batches * 10 rows each
        for (int i = 0; i < 30; i++)
        {
            assertThat(ids.get(i)).isEqualTo(i);
        }
    }

    @Test
    void testBulkReadFromSnapshot()
    {
        IInstance firstRunningInstance = cluster.getFirstRunningInstance();
        for (int i = 0; i < DATASET.size(); i++)
        {
            String query = String.format("INSERT INTO %s (id, value) VALUES (%d, '%s');", TABLE_SNAPSHOT, i, DATASET.get(i));
            firstRunningInstance.coordinator().execute(query, ConsistencyLevel.ALL);
        }
        cluster.stream().forEach(instance -> instance.nodetool("flush", TEST_KEYSPACE, TABLE_SNAPSHOT.table()));

        assertExpectedSSTableFormat(TABLE_SNAPSHOT);

        String snapshotName = "test_snapshot_" + System.currentTimeMillis();
        cluster.stream().forEach(instance -> instance.nodetool("snapshot", "-t", snapshotName, TEST_KEYSPACE));
        try
        {
            Dataset<Row> df = bulkReaderDataFrame(TABLE_SNAPSHOT)
                              .option("snapshotName", snapshotName)
                              .load();

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
        finally
        {
            cluster.stream().forEach(instance -> instance.nodetool("clearsnapshot", "-t", snapshotName, TEST_KEYSPACE));
        }
    }

    private void flushKeyspace()
    {
        cluster.stream().forEach(instance -> instance.nodetool("flush", TEST_KEYSPACE));
    }

    /**
     * Asserts the on-disk SSTables for the given table match the format produced by this test class:
     * {@code bti-da} for the BTI variant (5.x only), otherwise {@code big} with the version expected for the
     * Cassandra version under test ({@code oa} for 5.x, {@code nb} for 4.x).
     */
    private void assertExpectedSSTableFormat(QualifiedName table)
    {
        if ("bti".equals(sstableFormat()))
        {
            assertSSTableFormatOnDisk(table, "bti", "da");
        }
        else
        {
            String version = testVersion.version();
            String expectedSSTableVersion;
            if (version.startsWith("5."))
            {
                expectedSSTableVersion = "oa";
            }
            else if (version.startsWith("4."))
            {
                expectedSSTableVersion = "nb";
            }
            else
            {
                throw new IllegalStateException("Unsupported Cassandra version for SSTable format assertion: " + version);
            }
            assertSSTableFormatOnDisk(table, "big", expectedSSTableVersion);
        }
    }
}
