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

package org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf.ClusterConf;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedWriteConf.SimpleClusterConf;
import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel.CL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CoordinatedWriteConfTest
{
    @Test
    void testSerDeser()
    {
        Map<String, SimpleClusterConf> clusters = new HashMap<>();
        clusters.put("cluster1", new SimpleClusterConf(Arrays.asList("instance-1:9999", "instance-2:9999", "instance-3:9999"), "dc1"));
        clusters.put("cluster2", new SimpleClusterConf(Arrays.asList("instance-4:8888", "instance-5:8888", "instance-6:8888"), "dc1"));
        CoordinatedWriteConf conf = new CoordinatedWriteConf(clusters);
        String json = conf.toJson();
        assertThat(json)
        .isEqualTo("{\"cluster1\":" +
                   "{\"sidecarContactPoints\":[\"instance-1:9999\",\"instance-2:9999\",\"instance-3:9999\"]," +
                   "\"localDc\":\"dc1\"}," +
                   "\"cluster2\":" +
                   "{\"sidecarContactPoints\":[\"instance-4:8888\",\"instance-5:8888\",\"instance-6:8888\"]," +
                   "\"localDc\":\"dc1\"}}");
        CoordinatedWriteConf deser = CoordinatedWriteConf.create(json, CL.LOCAL_QUORUM, SimpleClusterConf.class);
        assertThat(deser.clusters()).containsKeys("cluster1", "cluster2");
        assertThat(deser.cluster("cluster1")).isNotNull();
        assertThatThrownBy(() -> deser.cluster("non-exist-cluster"))
        .isExactlyInstanceOf(NoSuchElementException.class)
        .hasMessage("No ClusterConf is found for clusterId: non-exist-cluster");
        assertThat(deser.clustersOf(SimpleClusterConf.class).get("cluster1").sidecarContactPointsValue())
        .isEqualTo(Arrays.asList("instance-1:9999", "instance-2:9999", "instance-3:9999"));
        assertThat(deser.clustersOf(SimpleClusterConf.class).get("cluster2").sidecarContactPointsValue())
        .isEqualTo(Arrays.asList("instance-4:8888", "instance-5:8888", "instance-6:8888"));
        assertThat(deser.clusters().get("cluster1").localDc()).isEqualTo("dc1");
        assertThat(deser.clusters().get("cluster2").localDc()).isEqualTo("dc1");
        Set<SidecarInstance> contactPoints = deser.clusters().get("cluster1").sidecarContactPoints();
        assertThat(contactPoints)
        .hasSize(3);
        // assertj contains method does not compile with SidecarInstanceImpl due to type erasure
        assertThat(contactPoints.contains(new SidecarInstanceImpl("instance-1", 9999))).isTrue();
        assertThat(contactPoints.contains(new SidecarInstanceImpl("instance-2", 9999))).isTrue();
        assertThat(contactPoints.contains(new SidecarInstanceImpl("instance-3", 9999))).isTrue();
        contactPoints = deser.clusters().get("cluster2").sidecarContactPoints();
        assertThat(contactPoints)
        .hasSize(3);
        assertThat(contactPoints.contains(new SidecarInstanceImpl("instance-4", 8888))).isTrue();
        assertThat(contactPoints.contains(new SidecarInstanceImpl("instance-5", 8888))).isTrue();
        assertThat(contactPoints.contains(new SidecarInstanceImpl("instance-6", 8888))).isTrue();
    }

    @Test
    void testDeserFailsWhenInstanceHasNoPort()
    {
        String json = "{\"cluster1\":" +
                      "{\"sidecarContactPoints\":[\"instance-1\",\"instance-2\",\"instance-3\"]," +
                      "\"localDc\":\"dc1\"}}";
        assertThatThrownBy(() -> CoordinatedWriteConf.create(json, CL.LOCAL_QUORUM, SimpleClusterConf.class))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unable to parse json string into CoordinatedWriteConf of SimpleClusterConf")
        .hasRootCauseExactlyInstanceOf(IllegalStateException.class)
        .hasRootCauseMessage("Unable to resolve port from instance-1");
    }

    @Test
    void testDeserFailsDueToMissingLocalDcWithNonLocalCL()
    {
        String json = "{\"cluster1\":{\"sidecarContactPoints\":[\"instance-1:8888\"]}}";
        assertThatThrownBy(() -> CoordinatedWriteConf.create(json, CL.LOCAL_QUORUM, SimpleClusterConf.class))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("localDc is not configured for cluster: cluster1 for consistency level: LOCAL_QUORUM");
    }

    @Test
    void testResolveLocalDc()
    {
        ClusterConf clusterWithLocalDc = new SimpleClusterConf(Collections.singletonList("instance-1:9999"), "dc1");
        assertThat(clusterWithLocalDc.resolveLocalDc(CL.EACH_QUORUM))
        .describedAs("Resolving localDc with Non-local CL should return null")
        .isNull();
        assertThat(clusterWithLocalDc.resolveLocalDc(CL.LOCAL_QUORUM))
        .describedAs("Resolving localDc with local CL should return the actual localDc")
        .isEqualTo("dc1");
        ClusterConf clusterWithoutLocalDc = new SimpleClusterConf(Collections.singletonList("instance-1:9999"), null);
        assertThat(clusterWithoutLocalDc.resolveLocalDc(CL.EACH_QUORUM)).isNull();
        assertThatThrownBy(() -> clusterWithoutLocalDc.resolveLocalDc(CL.LOCAL_QUORUM))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("No localDc is specified for local consistency level: " + CL.LOCAL_QUORUM);
    }
}
