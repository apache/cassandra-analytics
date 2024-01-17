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
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.TestVersion;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.CassandraTestTemplate.waitForHealthyRing;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotTtlTest extends SharedClusterSparkIntegrationTestBase
{
    static final QualifiedName TTL_NAME = new QualifiedName(TEST_KEYSPACE, "test_user_provided_ttl");
    static final List<String> DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h");

    @Test
    void testWithUserProvidedTTL()
    {
        DataFrameReader readDf = bulkReaderDataFrame(TTL_NAME)
                                 .option("snapshotName", "userProvidedSnapshotTTLTest")
                                 .option("clearSnapshot", "true")
                                 .option("snapshot_ttl", "1m");
        List<Row> rows = readDf.load().collectAsList();
        assertEquals(8, rows.size());

        String[] dataDirs = (String[]) cluster.getFirstRunningInstance()
                                              .config()
                                              .getParams()
                                              .get("data_file_directories");
        String dataDir = dataDirs[0];
        List<Path> snapshotPaths = findChildFile(Paths.get(dataDir), "userProvidedSnapshotTTLTest");
        assertFalse(snapshotPaths.isEmpty());
        Path snapshot = snapshotPaths.get(0);
        assertTrue(Files.exists(snapshot));

        // Wait to make sure TTLs have expired
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.MINUTES);
        assertFalse(Files.exists(snapshot));
    }

    @Test
    void testClearSnapshotOptionHonored()
    {
        DataFrameReader readDf = bulkReaderDataFrame(TTL_NAME)
                                 .option("snapshotName", "clearSnapshotHonorTest")
                                 .option("clearSnapshot", "false")
                                 .option("snapshot_ttl", "1m");
        List<Row> rows = readDf.load().collectAsList();
        assertEquals(8, rows.size());

        String[] dataDirs = (String[]) cluster.getFirstRunningInstance()
                                              .config()
                                              .getParams()
                                              .get("data_file_directories");
        String dataDir = dataDirs[0];
        List<Path> snapshotPaths = findChildFile(Paths.get(dataDir), "clearSnapshotHonorTest");
        assertFalse(snapshotPaths.isEmpty());
        Path snapshot = snapshotPaths.get(0);
        assertTrue(Files.exists(snapshot));

        // Wait to make sure TTLs have expired
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.MINUTES);
        assertTrue(Files.exists(snapshot));
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
        Versions versions = Versions.find();
        Versions.Version requestedVersion = versions.getLatest(new Semver(testVersion.version(),
                                                                          Semver.SemverType.LOOSE));
        UpgradeableCluster.Builder clusterBuilder =
        UpgradeableCluster.build(3)
                          .withDynamicPortAllocation(true)
                          .withVersion(requestedVersion)
                          .withDataDirCount(1)
                          .withDCs(1)
                          .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                      .with(Feature.GOSSIP)
                                                      .with(Feature.JMX));
        UpgradeableCluster cluster = clusterBuilder.createWithoutStarting();
        cluster.startup();

        waitForHealthyRing(cluster);
        return cluster;
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TTL_NAME, DC1_RF3);
        String createTableStatement = "CREATE TABLE IF NOT EXISTS %s (c1 int, c2 text, PRIMARY KEY(c1));";
        createTestTable(TTL_NAME, createTableStatement);
        populateTable();
    }

    void populateTable()
    {
        for (int i = 0; i < DATASET.size(); i++)
        {
            String value = DATASET.get(i);
            String query = String.format("INSERT INTO %s (c1, c2) VALUES (%d, '%s');", TTL_NAME, i, value);
            cluster.getFirstRunningInstance()
                   .coordinator()
                   .execute(query, ConsistencyLevel.ALL);
        }
    }
}
