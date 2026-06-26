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

package org.apache.cassandra.spark.bulkwriter;

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import org.apache.cassandra.bridge.CassandraVersion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CassandraClusterInfo#getBridgeVersion()} — the bridge-version priority chain:
 * version override (operator escape hatch) &gt; SSTable-version-based selection &gt; legacy cassandra.version.
 *
 * <p>No mocking framework is used for the cluster connectivity (an in-memory {@link CassandraContext} with no
 * Sidecar client); only the {@link BulkSparkConf} feature flag is stubbed. The cluster-derived inputs
 * (feature override, SSTable versions, lowest version) are supplied via a test subclass that also records
 * whether each was consulted.
 */
public class CassandraClusterInfoBridgeVersionTest
{
    @Test
    void testOverrideWinsWhenFeatureEnabled()
    {
        TestClusterInfo info = new TestClusterInfo(conf(false), "4.0.0", Collections.singleton("big-oa"), "5.0.0");

        assertThat(info.getBridgeVersion()).isEqualTo("4.0.0");
        // override short-circuits: neither SSTable versions nor the lowest version are consulted
        assertThat(info.sstableVersionsCalls).isEqualTo(0);
        assertThat(info.lowestVersionCalls).isEqualTo(0);
    }

    @Test
    void testOverrideWinsWhenFeatureDisabled()
    {
        TestClusterInfo info = new TestClusterInfo(conf(true), "4.0.0", Collections.singleton("big-oa"), "5.0.0");

        assertThat(info.getBridgeVersion()).isEqualTo("4.0.0");
        assertThat(info.sstableVersionsCalls).isEqualTo(0);
        assertThat(info.lowestVersionCalls).isEqualTo(0);
    }

    @Test
    void testSSTableVersionBasedWhenEnabledAndNoOverride()
    {
        // SSTable-version-based selection picks the lowest version whose SSTables every node can import.
        // The viable versions depend on the configured write format: bti is a 5.0-only format, so a bti job
        // resolves to 5.0; a big job with mixed 4.0 + 5.0 SSTables writes at the lowest importable (4.0).
        boolean bti = "bti".equals(CassandraVersion.configuredSSTableFormat());
        Set<String> sstableVersions = bti
                                      ? Collections.singleton("bti-da")
                                      : new java.util.HashSet<>(java.util.Arrays.asList("big-na", "big-oa"));
        CassandraVersion expected = bti ? CassandraVersion.FIVEZERO : CassandraVersion.FOURZERO;
        TestClusterInfo info = new TestClusterInfo(conf(false), null, sstableVersions, "5.0.0");

        assertThat(info.getBridgeVersion()).isEqualTo(expected.versionName() + ".0");
        assertThat(info.sstableVersionsCalls).isEqualTo(1);
        // SSTable-based selection does not consult the legacy lowest version
        assertThat(info.lowestVersionCalls).isEqualTo(0);
    }

    @Test
    void testLegacyVersionWhenDisabledAndNoOverride()
    {
        TestClusterInfo info = new TestClusterInfo(conf(true), null, Collections.singleton("big-oa"), "5.0.0");

        assertThat(info.getBridgeVersion()).isEqualTo("5.0.0");
        assertThat(info.lowestVersionCalls).isEqualTo(1);
        // legacy path does not consult SSTable versions
        assertThat(info.sstableVersionsCalls).isEqualTo(0);
    }

    private static BulkSparkConf conf(boolean sstableVersionBasedBridgeDisabled)
    {
        BulkSparkConf conf = mock(BulkSparkConf.class);
        when(conf.isSSTableVersionBasedBridgeDisabled()).thenReturn(sstableVersionBasedBridgeDisabled);
        return conf;
    }

    /**
     * {@link CassandraClusterInfo} backed by an in-memory {@link CassandraContext} (no Sidecar), with the
     * cluster-derived determination inputs supplied directly and call-counting so tests can assert which
     * branch of the priority chain was taken.
     */
    private static final class TestClusterInfo extends CassandraClusterInfo
    {
        private final String versionFromFeature;
        private final Set<String> sstableVersions;
        private final String lowestVersion;
        private int sstableVersionsCalls = 0;
        private int lowestVersionCalls = 0;

        TestClusterInfo(BulkSparkConf conf, String versionFromFeature, Set<String> sstableVersions, String lowestVersion)
        {
            super(conf);
            this.versionFromFeature = versionFromFeature;
            this.sstableVersions = sstableVersions;
            this.lowestVersion = lowestVersion;
        }

        @Override
        protected CassandraContext buildCassandraContext()
        {
            return new InMemoryCassandraContext();
        }

        @Override
        public String getVersionFromFeature()
        {
            return versionFromFeature;
        }

        @Override
        public Set<String> getSSTableVersionsOnCluster()
        {
            sstableVersionsCalls++;
            return sstableVersions;
        }

        @Override
        public String getVersionFromSidecar()
        {
            lowestVersionCalls++;
            return lowestVersion;
        }
    }

    /**
     * A {@link CassandraContext} with no real Sidecar connectivity: an empty cluster and a null client.
     * {@code Sidecar.allNodeSettings} is invoked with an empty instance set during construction, so the
     * null client is never dereferenced.
     */
    private static final class InMemoryCassandraContext extends CassandraContext
    {
        InMemoryCassandraContext()
        {
            super(null, null);
        }

        @Override
        protected Set<SidecarInstance> createClusterConfig()
        {
            return Collections.emptySet();
        }

        @Override
        protected SidecarClient initializeSidecarClient(BulkSparkConf conf)
        {
            return null;
        }
    }
}
