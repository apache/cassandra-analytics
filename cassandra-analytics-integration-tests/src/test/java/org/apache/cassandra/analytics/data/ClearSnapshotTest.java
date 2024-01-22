/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics.data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.TestVersion;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

class ClearSnapshotTest extends SharedClusterSparkIntegrationTestBase
{
    private static final WithProperties properties = new WithProperties();
    static final QualifiedName TABLE_NAME_FOR_TTL_CLEAR_SNAPSHOT_STRATEGY
    = new QualifiedName(TEST_KEYSPACE, "test_ttl_clear_snapshot_strategy");
    static final QualifiedName TABLE_NAME_FOR_NO_OP_CLEAR_SNAPSHOT_STRATEGY
    = new QualifiedName(TEST_KEYSPACE, "test_no_op_clear_snapshot_strategy");
    static final List<String> DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h");

    @Test
    void testTTLClearSnapshotStrategy()
    {
        DataFrameReader readDf = bulkReaderDataFrame(TABLE_NAME_FOR_TTL_CLEAR_SNAPSHOT_STRATEGY)
                                 .option("snapshotName", "ttlClearSnapshotStrategyTest")
                                 .option("clearSnapshotStrategy", "TTL 10s");
        List<Row> rows = readDf.load().collectAsList();
        assertThat(rows.size()).isEqualTo(8);

        String[] dataDirs = (String[]) cluster.getFirstRunningInstance()
                                              .config()
                                              .getParams()
                                              .get("data_file_directories");
        String dataDir = dataDirs[0];
        List<Path> snapshotPaths = findChildFile(Paths.get(dataDir), "ttlClearSnapshotStrategyTest");
        assertThat(snapshotPaths).isNotEmpty();
        Path snapshot = snapshotPaths.get(0);
        assertThat(snapshot).exists();

        // Wait up to 30 seconds to make sure files are cleared after TTLs have expired
        int wait = 0;
        while (Files.exists(snapshot) && wait++ < 30)
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        assertThat(snapshot).doesNotExist();
    }

    @Test
    void testNoOpClearSnapshotStrategy()
    {
        DataFrameReader readDf = bulkReaderDataFrame(TABLE_NAME_FOR_NO_OP_CLEAR_SNAPSHOT_STRATEGY)
                                 .option("snapshotName", "noOpClearSnapshotStrategyTest")
                                 .option("clearSnapshotStrategy", "noOp");
        List<Row> rows = readDf.load().collectAsList();
        assertThat(rows.size()).isEqualTo(8);

        String[] dataDirs = (String[]) cluster.getFirstRunningInstance()
                                              .config()
                                              .getParams()
                                              .get("data_file_directories");
        String dataDir = dataDirs[0];
        List<Path> snapshotPaths = findChildFile(Paths.get(dataDir), "noOpClearSnapshotStrategyTest");
        assertThat(snapshotPaths).isNotEmpty();
        Path snapshot = snapshotPaths.get(0);
        assertThat(snapshot).exists();

        Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
        assertThat(snapshot).exists();
    }

    private List<Path> findChildFile(Path path, String target)
    {
        try (Stream<Path> walkStream = Files.walk(path))
        {
            return walkStream.filter(p -> p.getFileName().endsWith(target) || p.toString().contains("/" + target + "/"))
                             .collect(Collectors.toList());
        }
        catch (IOException e)
        {
            return Collections.emptyList();
        }
    }

    @Override
    protected UpgradeableCluster provisionCluster(TestVersion testVersion) throws IOException
    {
        properties.set(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS, 0);
        properties.set(CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS, 1);
        properties.set(CassandraRelevantProperties.SNAPSHOT_MIN_ALLOWED_TTL_SECONDS, 5);

        Versions versions = Versions.find();
        Versions.Version requestedVersion = versions.getLatest(new Semver(testVersion.version(),
                                                                          Semver.SemverType.LOOSE));
        UpgradeableCluster.Builder clusterBuilder =
        UpgradeableCluster.build(1)
                          .withDynamicPortAllocation(true)
                          .withVersion(requestedVersion)
                          .withDataDirCount(1)
                          .withDCs(1)
                          .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                      .with(Feature.GOSSIP)
                                                      .with(Feature.JMX));

        return clusterBuilder.start();
    }

    @Override
    protected void afterClusterShutdown()
    {
        properties.close();
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        String createTableStatement = "CREATE TABLE IF NOT EXISTS %s (c1 int, c2 text, PRIMARY KEY(c1));";
        createTestTable(TABLE_NAME_FOR_TTL_CLEAR_SNAPSHOT_STRATEGY, createTableStatement);
        populateTable(TABLE_NAME_FOR_TTL_CLEAR_SNAPSHOT_STRATEGY);
        createTestTable(TABLE_NAME_FOR_NO_OP_CLEAR_SNAPSHOT_STRATEGY, createTableStatement);
        populateTable(TABLE_NAME_FOR_NO_OP_CLEAR_SNAPSHOT_STRATEGY);
    }

    void populateTable(QualifiedName tableName)
    {
        for (int i = 0; i < DATASET.size(); i++)
        {
            String value = DATASET.get(i);
            String query = String.format("INSERT INTO %s (c1, c2) VALUES (%d, '%s');", tableName, i, value);
            cluster.getFirstRunningInstance()
                   .coordinator()
                   .execute(query, ConsistencyLevel.ALL);
        }
    }
}
