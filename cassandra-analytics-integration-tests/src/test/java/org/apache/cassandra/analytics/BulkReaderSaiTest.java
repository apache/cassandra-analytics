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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.vdurmont.semver4j.Semver;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class BulkReaderSaiTest extends SharedClusterSparkIntegrationTestBase
{
    private static final QualifiedName TABLE = uniqueTestTableFullName(TEST_KEYSPACE);
    private static final QualifiedName QUOTED_TABLE = new QualifiedName(TEST_KEYSPACE, "TestQuotedTable", false, true);

    private static final int MATCHING_SCORE = 777;

    private static final int KEEP_ID = 1001;
    private static final int DELETE_ID = 1002;
    private static final int CHANGE_ID = 1003;
    private static final int NEW_MATCH_ID = 1004;

    // Make a full SSTable scan meaningfully larger than a point lookup.
    private static final int BACKGROUND_ROWS = 256;
    private static final String PAYLOAD = payload(16 * 1024);

    private static final CountingStats TEST_STATS = new CountingStats();

    private long fullScanBytes = -1;
    private ClassReloadingStrategy strategy = null;

    @Override
    protected void beforeClusterProvisioning()
    {
        System.setProperty("cassandra.analytics.bridges.sstable_format", sstableFormat());
        assumeThat(TestUtils.getDTestClusterVersion()
                            .isGreaterThanOrEqualTo(
                            new Semver("5.0", Semver.SemverType.LOOSE)))
        .describedAs("SAI requires Cassandra 5.0+").isTrue();
        ByteBuddyAgent.install();
    }

    @BeforeEach
    public void setUp() throws Exception
    {
        strategy = ClassReloadingStrategy.fromInstalledAgent();
        try
        {
            new ByteBuddy().redefine(CassandraDataLayer.class)
                           .method(ElementMatchers.named("stats"))
                           .intercept(MethodCall.invoke(BulkReaderSaiTest.class.getMethod("testStats")))
                           .make()
                           .load(CassandraDataLayer.class.getClassLoader(), strategy);
            TEST_STATS.reset();
        }
        catch (Exception e)
        {
            strategy.reset(CassandraDataLayer.class);
        }
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        strategy.reset(CassandraDataLayer.class);
    }

    /**
     * @return the SSTable format the analytics bulk writer should produce ({@code big} by default;
     * overridden to {@code bti} by the BTI variant).
     */
    protected String sstableFormat()
    {
        return "big";
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        ClusterBuilderConfiguration conf = super.testClusterConfiguration();
        // Preserve the base instance config (e.g. storage_compatibility_mode) and pin the node SSTable format.
        Map<String, Object> instanceConfig = new HashMap<>();
        if (conf.additionalInstanceConfig != null)
        {
            instanceConfig.putAll(conf.additionalInstanceConfig);
        }
        instanceConfig.put("sstable.selected_format", sstableFormat());
        return conf.additionalInstanceConfig(instanceConfig);
    }

    static Stream<Arguments> queryInputs()
    {
        return Stream.of(
        Arguments.of("eq", TABLE, "score = " + MATCHING_SCORE, 2, 0.08),
        Arguments.of("gt", TABLE, "score > 250", 8, 0.2),
        Arguments.of("between", TABLE, "score > 250 AND score < 300", 5, 0.1),
        Arguments.of("or", TABLE, "score <= 2 OR score > 300", 3 + 3, 1), // OR expressions do not use SAI index
        Arguments.of("multi_column", TABLE, "score = 778 AND state = 'changed'", 1, 0.05),
        Arguments.of("overridden", TABLE, "score = 777 AND state = 'change-score'", 0, 0.05), // not most recent value
        Arguments.of("quoted", QUOTED_TABLE, "Score > 250", 9, 0.2)
        );
    }

    @ParameterizedTest
    @MethodSource("queryInputs")
    void testIndexUsage(String view, QualifiedName table, String whereClause, int expectedRows, double maxDataFileReadPercentage)
    {
        calculateFullScanBaseline(table);

        // Alternative pushdown filter with Spark SQL context:
        // Dataset<Row> input = bulkReaderDataFrame(TABLE, Map.of("saiFilteringEnabled", "true")).load();
        // input.createOrReplaceTempView("sai_reader_" + view);
        // List<Row> matches = getOrCreateSparkSession().sql("SELECT id, score, state, payload "
        //                                                   + "FROM sai_reader_" + view
        //                                                   + " WHERE " + whereClause)
        //                                              .collectAsList();

        List<Row> matches = bulkReaderDataFrame(table, Map.of("saiFilteringEnabled", "true"))
                            .load()
                            .filter(whereClause)
                            .collectAsList();
        assertThat(matches).hasSize(expectedRows);

        long saiReadBytes = TEST_STATS.readBytes();
        // Prove we did not simply fall back to a full scan.
        assertThat(saiReadBytes).as("SAI predicate should substantially reduce Data.db reads; fullScanBytes=%s saiReadBytes=%s", fullScanBytes, saiReadBytes)
                                .isGreaterThan(0)
                                .isLessThanOrEqualTo((int) (fullScanBytes * maxDataFileReadPercentage));
    }

    public static Stats testStats()
    {
        return TEST_STATS;
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        String tableStmt = "CREATE TABLE IF NOT EXISTS %s ("
                           + "id int PRIMARY KEY, "
                           + "score int, "
                           + "state text, "
                           + "payload text"
                           + ") WITH compression = {'enabled': 'false'} "
                           + "AND compaction = {"
                           + "'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', "
                           + "'enabled': 'false'"
                           + "};";

        createTestTable(TABLE, tableStmt);

        createTestTable(QUOTED_TABLE, tableStmt
                                      .replace("score", "\"Score\"")
                                      .replace("state", "\"State\""));

        ICoordinator coordinator = cluster.get(1).coordinator();

        // standard
        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS score_sai ON %s (score) USING 'sai';", TABLE), ConsistencyLevel.ALL);
        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS state_sai ON %s (state) USING 'sai';", TABLE), ConsistencyLevel.ALL);

        // quoted
        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS \"Score_quoted_sai\" ON %s (\"Score\") USING 'sai';",
                                          QUOTED_TABLE), ConsistencyLevel.ALL);
        coordinator.execute(String.format("CREATE INDEX IF NOT EXISTS \"State_quoted_sai\" ON %s (\"State\") USING 'sai';",
                                          QUOTED_TABLE), ConsistencyLevel.ALL);

        // Lots of data which does not match score=777. The relatively
        // large payload gives us a robust full-scan I/O baseline.
        for (int i = 0; i < BACKGROUND_ROWS; i++)
        {
            insert(coordinator, i, i, "background");
        }

        // All three are SAI matches in SSTable #1.
        insert(coordinator, KEEP_ID, MATCHING_SCORE, "original");
        insert(coordinator, DELETE_ID, MATCHING_SCORE, "delete-me");
        insert(coordinator, CHANGE_ID, MATCHING_SCORE, "change-score");

        flushTable();

        // SSTable #2:
        //
        // KEEP_ID:
        //   Candidate comes from the older SAI index, but the newer
        //   non-indexed column value must win during reconciliation.
        coordinator.execute(String.format("UPDATE %s SET state = 'updated' WHERE id = %d", TABLE, KEEP_ID), ConsistencyLevel.ALL);

        // DELETE_ID:
        // Older SAI has a hit, newer SSTable contains a tombstone.
        coordinator.execute(String.format("DELETE FROM %s WHERE id = %d", TABLE, DELETE_ID), ConsistencyLevel.ALL);

        // CHANGE_ID:
        // Older SAI has score=777. New value is 778.
        // Spark's residual predicate must filter this partition.
        coordinator.execute(String.format("UPDATE %s SET score = 778, state = 'changed' WHERE id = %d", TABLE, CHANGE_ID), ConsistencyLevel.ALL);

        // Match existing only in the second SSTable.
        insert(coordinator, NEW_MATCH_ID, MATCHING_SCORE, "new-match");

        flushTable();
    }

    private static void insert(ICoordinator coordinator,
                               int id,
                               int score,
                               String state)
    {
        coordinator.execute(String.format("INSERT INTO %s (id, score, state, payload) VALUES (%d, %d, '%s', '%s')",
                                          TABLE, id, score, state, PAYLOAD),
                            ConsistencyLevel.ALL);
        coordinator.execute(String.format("INSERT INTO %s (id, \"Score\", \"State\", payload) VALUES (%d, %d, '%s', '%s')",
                                          QUOTED_TABLE, id, score, state, PAYLOAD),
                            ConsistencyLevel.ALL);
    }

    private void flushTable()
    {
        cluster.stream()
               .forEach(instance ->
                        instance.nodetool("flush", TEST_KEYSPACE, TABLE.table()));
    }

    private static String payload(int length)
    {
        char[] chars = new char[length];
        Arrays.fill(chars, 'x');
        return new String(chars);
    }

    private void calculateFullScanBaseline(QualifiedName table)
    {
        if (fullScanBytes < 0)
        {
            List<Row> allRows = bulkReaderDataFrame(table, Map.of("saiFilteringEnabled", "false"))
                                .load()
                                .select("id", "score", "state", "payload")
                                .collectAsList();
            assertThat(allRows).hasSize(BACKGROUND_ROWS + 3);

            fullScanBytes = TEST_STATS.readBytes();
            assertThat(fullScanBytes).as("full scan should read a meaningful amount of Data.db")
                                     .isGreaterThan(1024 * 1024);

            TEST_STATS.reset();
        }
    }

    private static final class CountingStats extends Stats
    {
        private final AtomicLong bytes = new AtomicLong();

        @Override
        public void readBytes(int length)
        {
            bytes.addAndGet(length);
        }

        long readBytes()
        {
            return bytes.get();
        }

        void reset()
        {
            bytes.set(0);
        }
    }
}
