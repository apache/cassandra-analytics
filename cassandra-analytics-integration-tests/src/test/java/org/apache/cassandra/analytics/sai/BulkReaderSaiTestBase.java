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
package org.apache.cassandra.analytics.sai;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import com.vdurmont.semver4j.Semver;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase;
import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public abstract class BulkReaderSaiTestBase extends SharedClusterSparkIntegrationTestBase
{
    protected static final CountingStats TEST_STATS = new CountingStats();

    protected long fullScanBytes = -1;
    protected ClassReloadingStrategy strategy = null;

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
                           .intercept(MethodCall.invoke(BulkReaderSaiTestBase.class.getMethod("testStats")))
                           .make()
                           .load(CassandraDataLayer.class.getClassLoader(), strategy);
            TEST_STATS.reset();
        }
        catch (Exception e)
        {
            strategy.reset(CassandraDataLayer.class);
        }
        calculateFullScanBaseline();
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

    public static Stats testStats()
    {
        return TEST_STATS;
    }

    protected long bytesRead()
    {
        return TEST_STATS.readBytes();
    }

    protected void flushTable()
    {
        cluster.stream()
               .forEach(instance ->
                        instance.nodetool("flush", TEST_KEYSPACE, testTable().table()));
    }

    protected static String payload(int length)
    {
        char[] chars = new char[length];
        Arrays.fill(chars, 'x');
        return new String(chars);
    }

    protected abstract QualifiedName testTable();

    protected abstract int expectedRowCount();

    private void calculateFullScanBaseline()
    {
        if (fullScanBytes < 0)
        {
            List<Row> allRows = bulkReaderDataFrame(testTable(), Map.of("saiFilteringEnabled", "false"))
                                .load()
                                .collectAsList();
            assertThat(allRows).hasSize(expectedRowCount());

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
