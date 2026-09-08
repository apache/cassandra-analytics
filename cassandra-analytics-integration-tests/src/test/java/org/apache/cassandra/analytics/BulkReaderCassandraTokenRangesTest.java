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

package org.apache.cassandra.analytics;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests bulk reads that source token ranges from Cassandra via the token-range-replicas endpoint, rather than
 * deriving them locally from tokens and the replication factor.
 * <p>
 * Mutation tracked keyspaces take that path automatically, because a range may be replicated to a witness holding
 * no data and the local derivation cannot say which instance replicates which range. Tracked keyspaces cannot be
 * created until Cassandra 6.0 bridge modules land (CASSANALYTICS-192), so these tests use the
 * {@code forcecassandratokenranges} option to exercise the same path against an ordinary keyspace.
 * <p>
 * Reads must return exactly the same data either way, so each test asserts the full dataset rather than only a row
 * count - an incomplete read is the failure mode this path exists to prevent.
 */
class BulkReaderCassandraTokenRangesTest extends SharedClusterSparkIntegrationTestBase
{
    static final List<String> DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g");

    QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE);

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3);
    }

    @Test
    void testReadWithCassandraSuppliedTokenRanges()
    {
        Dataset<Row> data = bulkReaderDataFrame(table, Collections.singletonMap("forcecassandratokenranges", "true"))
                            .load();
        assertCompleteDataset(data);
    }

    /**
     * The derived and Cassandra-supplied paths must agree. This is the check that would catch the ranges being
     * misaligned, which for a witness-enabled keyspace would show up as silently missing rows.
     */
    @Test
    void testCassandraSuppliedRangesMatchDerivedRanges()
    {
        Dataset<Row> derived = bulkReaderDataFrame(table).load();
        Dataset<Row> supplied = bulkReaderDataFrame(table, Collections.singletonMap("forcecassandratokenranges", "true"))
                                .load();

        assertCompleteDataset(derived);
        assertCompleteDataset(supplied);
        assertThat(sortedNames(supplied))
        .describedAs("reads with Cassandra-supplied ranges must return the same rows as derived ranges")
        .isEqualTo(sortedNames(derived));
    }

    @Test
    void testReadWithCassandraSuppliedTokenRangesAtQuorum()
    {
        Dataset<Row> data = bulkReaderDataFrame(table, Collections.singletonMap("forcecassandratokenranges", "true"))
                            .option("consistencyLevel", "LOCAL_QUORUM")
                            .load();
        assertCompleteDataset(data);
    }

    /**
     * Pushdown filters are applied against the Spark partition token ranges, which are derived from the ring's
     * ranges, so they are worth exercising on the new path.
     */
    @Test
    void testPushDownFilterWithCassandraSuppliedTokenRanges()
    {
        Dataset<Row> data = bulkReaderDataFrame(table, Collections.singletonMap("forcecassandratokenranges", "true"))
                            .load()
                            .filter("id = 3");

        List<Row> rows = data.collectAsList();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(3);
        assertThat(rows.get(0).getString(1)).isEqualTo(DATASET.get(3));
    }

    private void assertCompleteDataset(Dataset<Row> data)
    {
        List<Row> rows = data.collectAsList().stream()
                             .sorted(Comparator.comparing(row -> row.getInt(0)))
                             .collect(Collectors.toList());
        assertThat(rows).hasSize(DATASET.size());
        for (int i = 0; i < DATASET.size(); i++)
        {
            assertThat(rows.get(i).getInt(0)).isEqualTo(i);
            assertThat(rows.get(i).getString(1)).isEqualTo(DATASET.get(i));
        }
    }

    private List<String> sortedNames(Dataset<Row> data)
    {
        return data.collectAsList().stream()
                   .map(row -> row.getInt(0) + "=" + row.getString(1))
                   .sorted()
                   .collect(Collectors.toList());
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        createTestTable(table, "CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, name text);");

        IInstance firstRunningInstance = cluster.getFirstRunningInstance();
        for (int i = 0; i < DATASET.size(); i++)
        {
            firstRunningInstance.coordinator()
                                .execute(String.format("INSERT INTO %s (id, name) VALUES (%d, '%s');",
                                                       table, i, DATASET.get(i)),
                                         ConsistencyLevel.ALL);
        }
    }
}
