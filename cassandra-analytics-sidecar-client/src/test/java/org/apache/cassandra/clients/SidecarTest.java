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

package org.apache.cassandra.clients;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarInstance;
import o.a.c.sidecar.client.shaded.client.SidecarInstanceImpl;
import o.a.c.sidecar.client.shaded.common.response.GossipInfoResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for Sidecar utility methods
 */
public class SidecarTest
{
    @Test
    void testGetSSTableVersionsFromClusterWithSingleNode()
    {
        SidecarClient client = mock(SidecarClient.class);
        SidecarInstance instance = new SidecarInstanceImpl("localhost", 9043);
        Set<SidecarInstance> instances = Collections.singleton(instance);

        // Mock gossip response
        GossipInfoResponse response = new GossipInfoResponse();
        GossipInfoResponse.GossipInfo info = createGossipInfo(Arrays.asList("big-na", "big-nb"));
        response.put("localhost", info);

        when(client.gossipInfo(any(SidecarInstance.class)))
            .thenReturn(CompletableFuture.completedFuture(response));

        Set<String> versions = Sidecar.getSSTableVersionsFromCluster(client, instances, 1000L, 3);

        assertThat(versions)
            .describedAs("Should extract SSTable versions from gossip info")
            .containsExactlyInAnyOrder("big-na", "big-nb");
    }

    @Test
    void testGetSSTableVersionsFromClusterWithMultipleNodesAndMixedVersions()
    {
        SidecarClient client = mock(SidecarClient.class);
        SidecarInstance instance1 = new SidecarInstanceImpl("node1", 9043);
        SidecarInstance instance2 = new SidecarInstanceImpl("node2", 9043);
        Set<SidecarInstance> instances = new HashSet<>(Arrays.asList(instance1, instance2));

        // Node1 has C* 4.0 versions (big-na, big-nb)
        GossipInfoResponse response1 = new GossipInfoResponse();
        GossipInfoResponse.GossipInfo info1 = createGossipInfo(Arrays.asList("big-na", "big-nb"));
        response1.put("node1", info1);

        // Node2 has mixed versions: one C* 4.0 (big-nb) and C* 5.0 versions (big-oa, bti-da)
        GossipInfoResponse response2 = new GossipInfoResponse();
        GossipInfoResponse.GossipInfo info2 = createGossipInfo(Arrays.asList("big-nb", "big-oa", "bti-da"));
        response2.put("node2", info2);

        when(client.gossipInfo(instance1))
            .thenReturn(CompletableFuture.completedFuture(response1));
        when(client.gossipInfo(instance2))
            .thenReturn(CompletableFuture.completedFuture(response2));

        Set<String> versions = Sidecar.getSSTableVersionsFromCluster(client, instances, 1000L, 3);

        assertThat(versions)
            .describedAs("Should aggregate and deduplicate SSTable versions from multiple nodes with mixed Cassandra versions")
            .containsExactlyInAnyOrder("big-na", "big-nb", "big-oa", "bti-da");
    }

    @Test
    void testGetSSTableVersionsFromClusterWithFailedNode()
    {
        SidecarClient client = mock(SidecarClient.class);
        SidecarInstance instance1 = new SidecarInstanceImpl("node1", 9043);
        SidecarInstance instance2 = new SidecarInstanceImpl("node2", 9043);
        Set<SidecarInstance> instances = new HashSet<>(Arrays.asList(instance1, instance2));

        // Node1 succeeds
        GossipInfoResponse response1 = new GossipInfoResponse();
        GossipInfoResponse.GossipInfo info1 = createGossipInfo(Arrays.asList("big-na", "big-nb"));
        response1.put("node1", info1);

        when(client.gossipInfo(instance1))
            .thenReturn(CompletableFuture.completedFuture(response1));

        // Node2 fails
        when(client.gossipInfo(instance2))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")));

        Set<String> versions = Sidecar.getSSTableVersionsFromCluster(client, instances, 1000L, 3);

        assertThat(versions)
            .describedAs("Should return versions from successful nodes even when some fail")
            .containsExactlyInAnyOrder("big-na", "big-nb");
    }

    @Test
    void testGetSSTableVersionsFromClusterWhenAllNodesFail()
    {
        SidecarClient client = mock(SidecarClient.class);
        SidecarInstance instance1 = new SidecarInstanceImpl("node1", 9043);
        SidecarInstance instance2 = new SidecarInstanceImpl("node2", 9043);
        Set<SidecarInstance> instances = new HashSet<>(Arrays.asList(instance1, instance2));

        // Both nodes fail
        when(client.gossipInfo(any(SidecarInstance.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")));

        Set<String> versions = Sidecar.getSSTableVersionsFromCluster(client, instances, 1000L, 3);

        assertThat(versions)
            .describedAs("Should return empty set when all nodes fail")
            .isEmpty();
    }

    @Test
    void testGetSSTableVersionsFromClusterWithNullSSTableVersions()
    {
        SidecarClient client = mock(SidecarClient.class);
        SidecarInstance instance = new SidecarInstanceImpl("localhost", 9043);
        Set<SidecarInstance> instances = Collections.singleton(instance);

        // Mock gossip response with null SSTable versions
        GossipInfoResponse response = new GossipInfoResponse();
        GossipInfoResponse.GossipInfo info = createGossipInfo(null);
        response.put("localhost", info);

        when(client.gossipInfo(any(SidecarInstance.class)))
            .thenReturn(CompletableFuture.completedFuture(response));

        Set<String> versions = Sidecar.getSSTableVersionsFromCluster(client, instances, 1000L, 3);

        assertThat(versions)
            .describedAs("Should return empty set when SSTable versions are null")
            .isEmpty();
    }

    @Test
    void testGetSSTableVersionsFromClusterWithEmptyInstancesSet()
    {
        SidecarClient client = mock(SidecarClient.class);
        Set<SidecarInstance> instances = Collections.emptySet();

        Set<String> versions = Sidecar.getSSTableVersionsFromCluster(client, instances, 1000L, 3);

        assertThat(versions)
            .describedAs("Should return empty set when no instances provided")
            .isEmpty();
    }

    private GossipInfoResponse.GossipInfo createGossipInfo(List<String> sstableVersions)
    {
        GossipInfoResponse.GossipInfo info = new GossipInfoResponse.GossipInfo();
        info.put("status", "NORMAL");
        info.put("rack", "RACK1");
        info.put("datacenter", "DC1");
        info.put("releaseVersion", "4.0.0");
        info.put("rpcAddress", "127.0.0.1");
        if (sstableVersions != null && !sstableVersions.isEmpty())
        {
            info.put("sstableVersions", String.join(",", sstableVersions));
        }
        return info;
    }
}
