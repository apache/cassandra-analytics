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

package org.apache.cassandra.spark.data;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.SSTableVersionAnalyzer;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.utils.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CassandraDataLayer validation methods
 */
public class CassandraDataLayerValidationTest
{
    @Test
    void testValidateSSTableVersionsListWithValidVersions()
    {
        Set<String> expectedVersions = new HashSet<>(Arrays.asList("big-na", "big-nb"));
        CassandraDataLayer dataLayer = createTestDataLayerWithVersions(expectedVersions);

        SSTable ssTable1 = createMockSSTable("big", "na", "test1-big-na-Data.db");
        SSTable ssTable2 = createMockSSTable("big", "nb", "test2-big-nb-Data.db");
        List<SSTable> sstables = Arrays.asList(ssTable1, ssTable2);

        assertThatNoException()
        .describedAs("All SSTables have expected versions")
        .isThrownBy(() -> dataLayer.validateSSTableVersions(sstables));
    }

    @Test
    void testValidateSSTableVersionsAcceptsReadableVersionNotInGossip()
    {
        // Only big-na was observed in gossip, but the selected read bridge (4.0) can also read big-nb. A version
        // that appears after the gossip snapshot but is still readable by the bridge must not fail validation.
        Set<String> expectedVersions = new HashSet<>(List.of("big-na"));
        CassandraDataLayer dataLayer = createTestDataLayerWithVersions(expectedVersions);

        SSTable ssTable1 = createMockSSTable("big", "na", "test1-big-na-Data.db");
        SSTable ssTable2 = createMockSSTable("big", "nb", "test2-big-nb-Data.db");
        List<SSTable> sstables = Arrays.asList(ssTable1, ssTable2);

        assertThatNoException()
        .describedAs("A version readable by the selected bridge but not in the gossip snapshot should be accepted")
        .isThrownBy(() -> dataLayer.validateSSTableVersions(sstables));
    }

    @Test
    void testValidateSSTableVersionsListWithEmptyList()
    {
        Set<String> expectedVersions = new HashSet<>(List.of("big-na"));
        CassandraDataLayer dataLayer = createTestDataLayerWithVersions(expectedVersions);

        assertThatNoException()
        .describedAs("Empty SSTable list should not throw exception")
        .isThrownBy(() -> dataLayer.validateSSTableVersions(Collections.emptyList()));
    }

    @Test
    void testValidateSSTableVersionsListErrorMessageIncludesFileName()
    {
        Set<String> expectedVersions = new HashSet<>(List.of("big-na"));
        CassandraDataLayer dataLayer = createTestDataLayerWithVersions(expectedVersions);

        SSTable ssTable = createMockSSTable("big", "oa", "keyspace-table-big-oa-Data.db");
        List<SSTable> sstables = Collections.singletonList(ssTable);

        assertThatThrownBy(() -> dataLayer.validateSSTableVersions(sstables))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("keyspace-table-big-oa-Data.db")
        .hasMessageContaining("Versions observed in gossip:")
        .hasMessageContaining("set spark.cassandra_analytics.bridge.disable_sstable_version_based=true");
    }

    @Test
    void testValidateSSTableVersionsListWithMixedBigAndBtiFormats()
    {
        Set<String> expectedVersions = new HashSet<>(Arrays.asList("big-oa", "bti-da"));
        CassandraDataLayer dataLayer = createTestDataLayerWithVersions(expectedVersions);

        SSTable ssTable1 = createMockSSTable("big", "oa", "test1-big-oa-Data.db");
        SSTable ssTable2 = createMockSSTable("bti", "da", "test2-bti-da-Data.db");
        List<SSTable> sstables = Arrays.asList(ssTable1, ssTable2);

        assertThatNoException()
        .describedAs("Mixed big and bti format SSTables should be validated successfully")
        .isThrownBy(() -> dataLayer.validateSSTableVersions(sstables));
    }

    @Test
    void testValidateSSTableVersionsRejectsUnreadableOlderVersion()
    {
        // The selected read bridge (5.0, from big-oa) cannot read 3.x SSTables (big-mf), so it must be rejected
        // even though both are "big" format.
        Set<String> expectedVersions = new HashSet<>(List.of("big-oa"));
        CassandraDataLayer dataLayer = createTestDataLayerWithVersions(expectedVersions);

        SSTable ssTable1 = createMockSSTable("big", "oa", "test1-big-oa-Data.db");
        SSTable ssTable2 = createMockSSTable("big", "mf", "keyspace-table-big-mf-Data.db");
        List<SSTable> sstables = Arrays.asList(ssTable1, ssTable2);

        assertThatThrownBy(() -> dataLayer.validateSSTableVersions(sstables))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("has version 'big-mf' which is not readable by the bridge")
        .hasMessageContaining("keyspace-table-big-mf-Data.db")
        .hasMessageContaining("Versions observed in gossip:")
        .hasMessageContaining("set spark.cassandra_analytics.bridge.disable_sstable_version_based=true");
    }

    @Test
    void testValidateSSTableVersionsListSkipsValidationWhenFeatureDisabled()
    {
        // When the feature is disabled the driver leaves sstableVersionsOnCluster empty; on the executor
        // an empty expected set is the signal that the feature is disabled, so validation is skipped.
        CassandraDataLayer dataLayer = createTestDataLayerWithVersions(Collections.emptySet());

        // SSTable has a version that would be rejected if validation actually ran
        SSTable ssTable = createMockSSTable("big", "oa", "test-big-oa-Data.db");
        List<SSTable> sstables = Collections.singletonList(ssTable);

        assertThatNoException()
        .describedAs("Validation should be skipped when feature is disabled (empty expected versions)")
        .isThrownBy(() -> dataLayer.validateSSTableVersions(sstables));
    }

    // Tests for initializeSSTableVersionsAndBridgeVersion (called from initBulkReader - lines 283-313)

    @Test
    void testInitializeSSTableVersionsAndBridgeVersionWithFeatureEnabled()
    {
        Set<String> mockVersions = new HashSet<>(Arrays.asList("big-na", "big-nb"));
        CassandraDataLayer dataLayer = new TestCassandraDataLayerForInitTests(mockVersions, false);

        CassandraVersion result = dataLayer.initializeSSTableVersionsAndBridgeVersion("4.0.0");

        // Should return FOURZERO bridge version
        assertThat((Object) result).isEqualTo(CassandraVersion.FOURZERO);

        // Should set sstableVersionsOnCluster to the retrieved versions
        assertThat((Object) dataLayer.sstableVersionsOnCluster)
        .isNotNull();
        assertThat(dataLayer.sstableVersionsOnCluster)
        .containsExactlyInAnyOrder("big-na", "big-nb");
    }

    @Test
    void testInitializeSSTableVersionsAndBridgeVersionWithFeatureDisabled()
    {
        // Versions won't be retrieved when feature is disabled
        CassandraDataLayer dataLayer = new TestCassandraDataLayerForInitTests(null, true);

        CassandraVersion result = dataLayer.initializeSSTableVersionsAndBridgeVersion("4.0.0");

        // Should return FOURZERO bridge version (legacy bridge selection)
        assertThat((Object) result).isEqualTo(CassandraVersion.FOURZERO);

        // Should set sstableVersionsOnCluster to an empty set (skipped retrieval, never null)
        assertThat(dataLayer.sstableVersionsOnCluster).isEmpty();
    }

    @Test
    void testInitializeSSTableVersionsAndBridgeVersionSelectsFiveZeroBridge()
    {
        Set<String> mockVersions = new HashSet<>(Arrays.asList("big-oa", "bti-da"));
        CassandraDataLayer dataLayer = new TestCassandraDataLayerForInitTests(mockVersions, false);

        CassandraVersion result = dataLayer.initializeSSTableVersionsAndBridgeVersion("5.0.0");

        // Should return FIVEZERO bridge version based on highest SSTable version
        assertThat((Object) result).isEqualTo(CassandraVersion.FIVEZERO);

        // Should set sstableVersionsOnCluster to the retrieved versions
        assertThat((Object) dataLayer.sstableVersionsOnCluster)
        .isNotNull();
        assertThat(dataLayer.sstableVersionsOnCluster)
        .containsExactlyInAnyOrder("big-oa", "bti-da");
    }

    @Test
    void testInitializeSSTableVersionsAndBridgeVersionWithMixedVersions()
    {
        // Cluster has both C* 4.0 and C* 5.0 versions (during rolling upgrade)
        Set<String> mockVersions = new HashSet<>(Arrays.asList("big-na", "big-oa"));
        CassandraDataLayer dataLayer = new TestCassandraDataLayerForInitTests(mockVersions, false);

        CassandraVersion result = dataLayer.initializeSSTableVersionsAndBridgeVersion("5.0.0");

        // Should return FIVEZERO bridge (highest version)
        assertThat((Object) result).isEqualTo(CassandraVersion.FIVEZERO);

        // Should set sstableVersionsOnCluster to all retrieved versions
        assertThat((Object) dataLayer.sstableVersionsOnCluster)
        .isNotNull();
        assertThat(dataLayer.sstableVersionsOnCluster)
        .containsExactlyInAnyOrder("big-na", "big-oa");
    }

    @Test
    void testInitializeSSTableVersionsAndBridgeVersionFailsFastWhenEnabledAndNoVersions()
    {
        // Feature enabled but the cluster returns no SSTable versions -> fail fast with an actionable hint
        CassandraDataLayer dataLayer = new TestCassandraDataLayerForInitTests(Collections.emptySet(), false);

        assertThatThrownBy(() -> dataLayer.initializeSSTableVersionsAndBridgeVersion("5.0.0"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unable to retrieve SSTable versions from cluster")
        .hasMessageContaining("set spark.cassandra_analytics.bridge.disable_sstable_version_based=true");
    }

    private CassandraDataLayer createTestDataLayerWithVersions(Set<String> sstableVersions)
    {
        return new TestCassandraDataLayer(sstableVersions);
    }

    private SSTable createMockSSTable(String format, String version, String fileName)
    {
        SSTable ssTable = mock(SSTable.class);
        when(ssTable.getFormat()).thenReturn(format);
        when(ssTable.getVersion()).thenReturn(version);
        when(ssTable.getDataFileName()).thenReturn(fileName);
        return ssTable;
    }

    /**
     * Test subclass to allow setting sstableVersionsOnCluster for testing
     * This subclass uses the serialization constructor and overrides problematic methods
     */
    private static class TestCassandraDataLayer extends CassandraDataLayer
    {
        TestCassandraDataLayer(Set<String> sstableVersions)
        {
            // Use the serialization constructor which doesn't call initialize()
            super("test_keyspace",                    // keyspace
                  "test_table",                       // table
                  false,                              // quoteIdentifiers
                  "",                                 // snapshotName
                  null,                               // datacenter
                  Sidecar.ClientConfig.create(),      // sidecarClientConfig
                  null,                               // sslConfig
                  mock(CqlTable.class),               // cqlTable
                  mock(TokenPartitioner.class),       // tokenPartitioner
                  bridgeVersionFor(sstableVersions),  // bridgeVersion (matches what the driver would select)
                  ConsistencyLevel.LOCAL_QUORUM,      // consistencyLevel
                  "127.0.0.1",                        // sidecarInstances
                  9043,                               // sidecarPort
                  Collections.emptyMap(),             // availabilityHints
                  Collections.emptyMap(),             // bigNumberConfigMap
                  false,                              // enableStats
                  false,                              // readIndexOffset
                  false,                              // useIncrementalRepair
                  null,                               // lastModifiedTimestampField
                  Collections.emptyList(),            // requestedFeatures
                  Collections.emptyMap(),             // rfMap
                  mock(TimeProvider.class),           // timeProvider
                  null,                               // sstableTimeRangeFilter
                  sstableVersions);                   // sstableVersionsOnCluster
            this.sstableVersionsOnCluster = sstableVersions;
        }

        /**
         * Derives the bridge version the driver would select for these SSTable versions, so that
         * {@code bridge().getVersion()} (used by validateSSTableVersions) matches the scenario. Falls back to
         * 4.0 when no versions are supplied (disabled-feature / init-only cases).
         */
        private static CassandraVersion bridgeVersionFor(Set<String> sstableVersions)
        {
            return (sstableVersions == null || sstableVersions.isEmpty())
                   ? CassandraVersion.FOURZERO
                   : SSTableVersionAnalyzer.determineBridgeVersionForRead(sstableVersions);
        }

        @Override
        public void startupValidate()
        {
            // Skip startup validation in tests to avoid SparkContext initialization
        }

        @Override
        protected void initSidecarClient()
        {
            // Skip sidecar client initialization in tests
        }

        @Override
        protected void initInstanceMap()
        {
            // Skip instance map initialization in tests
        }

        @Override
        protected boolean isSSTableVersionBasedBridgeDisabled()
        {
            // Override to avoid reading from the active SparkSession, which is not available in unit tests.
            // Always return false to test the validation logic
            return false;
        }
    }

    /**
     * Test subclass for testing initializeSSTableVersionsAndBridgeVersion method
     * Mocks the Sidecar interaction to avoid actual cluster calls
     */
    private static class TestCassandraDataLayerForInitTests extends TestCassandraDataLayer
    {
        private final Set<String> mockSSTableVersions;
        private final boolean featureDisabled;

        TestCassandraDataLayerForInitTests(Set<String> mockSSTableVersions, boolean featureDisabled)
        {
            super(null);
            this.mockSSTableVersions = mockSSTableVersions;
            this.featureDisabled = featureDisabled;
        }

        @Override
        protected boolean isSSTableVersionBasedBridgeDisabled()
        {
            return featureDisabled;
        }

        @Override
        protected Set<String> getSSTableVersionsFromCluster()
        {
            // Mock the Sidecar call - return the mock versions instead of calling actual sidecar
            return mockSSTableVersions;
        }
    }
}
