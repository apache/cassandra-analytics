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
import org.apache.cassandra.bridge.SSTableVersionAnalyzer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test that verifies the {@link CassandraClusterInfo#getVersionFromFeature()} override is honored in the
 * legacy version-based bridge selection path (when SSTable version-based bridge selection is disabled).
 *
 * <p>{@link CassandraClusterInfo#getLowestCassandraVersion()} returns the override when it is non-null,
 * short-circuiting any Sidecar/node-settings lookup. In legacy mode that value is what
 * {@link SSTableVersionAnalyzer#determineBridgeVersionForWrite} uses to select the bridge
 * (see {@code SSTableVersionAnalyzerTest} for the full version-to-bridge coverage).
 *
 * <p>The override is a subclass extension hook rather than a config flag, so it is exercised here via a
 * test subclass. No mocking framework is used: the cluster info is backed by a lightweight in-memory
 * {@link CassandraContext} (empty cluster, no Sidecar client).
 */
public class VersionFeatureLegacyBridgeTest
{
    @Test
    void testFeatureOverrideDrivesLowestCassandraVersionWithoutQueryingCluster()
    {
        // The in-memory context has no nodes; if the override were ignored, getLowestCassandraVersion() would
        // attempt to resolve node settings and fail, so returning the override proves it short-circuits.
        CassandraClusterInfo clusterInfo = new FeatureOverrideClusterInfo("4.0.0");

        assertThat(clusterInfo.getLowestCassandraVersion())
        .describedAs("getVersionFromFeature() override must take precedence over cluster-derived version")
        .isEqualTo("4.0.0");
    }

    @Test
    void testFeatureOverrideSelectsLegacyBridge()
    {
        // bridgeVersion field is intentionally FIVEZERO (see the subclass); the legacy bridge must instead
        // come from the feature override (4.0.0 -> FOURZERO), proving the override is what drives selection.
        CassandraClusterInfo clusterInfo = new FeatureOverrideClusterInfo("4.0.0");

        // Mirror AbstractBulkWriterContext's legacy flow: sstableVersionsOnCluster is null when disabled,
        // and the bridge is derived solely from getLowestCassandraVersion().
        CassandraVersion bridge = SSTableVersionAnalyzer.determineBridgeVersionForWrite(
            null, "big", clusterInfo.getLowestCassandraVersion(), /* isSSTableVersionBasedBridgeDisabled */ true);

        assertThat(bridge)
        .describedAs("In legacy mode the bridge must come from the feature-overridden version")
        .isEqualTo(CassandraVersion.FOURZERO);
    }

    /**
     * {@link CassandraClusterInfo} that supplies a fixed {@link #getVersionFromFeature()} value and is backed
     * by an in-memory {@link CassandraContext} so no real cluster connectivity is required.
     */
    private static final class FeatureOverrideClusterInfo extends CassandraClusterInfo
    {
        private final String featureVersion;

        FeatureOverrideClusterInfo(String featureVersion)
        {
            // bridgeVersion deliberately differs from the override version under test
            super((BulkSparkConf) null, CassandraVersion.FIVEZERO);
            this.featureVersion = featureVersion;
        }

        @Override
        protected CassandraContext buildCassandraContext()
        {
            return new InMemoryCassandraContext();
        }

        @Override
        public String getVersionFromFeature()
        {
            return featureVersion;
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
