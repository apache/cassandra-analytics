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

package org.apache.cassandra.spark.utils;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.RangeMap;
import org.junit.jupiter.api.Test;

import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TokenRangeUtilsTest
{
    @Test
    void testConvertTokenRangeReplicasToRangeMapBasic()
    {
        // Create test instances
        List<CassandraInstance> instances = Arrays.asList(
        new CassandraInstance("-1000", "node1.example.com", "DC1"),
        new CassandraInstance("0", "node2.example.com", "DC1"),
        new CassandraInstance("1000", "node3.example.com", "DC2")
        );

        // Create replica metadata using actual class
        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = new HashMap<>();
        metadata.put("192.168.1.1:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node1.example.com", "192.168.1.1", 9042, "DC1"));
        metadata.put("192.168.1.2:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node2.example.com", "192.168.1.2", 9042, "DC1"));
        metadata.put("192.168.1.3:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node3.example.com", "192.168.1.3", 9042, "DC2"));

        // Create replica info using actual class
        Map<String, List<String>> replicasByDatacenter1 = new HashMap<>();
        replicasByDatacenter1.put("DC1", Arrays.asList("192.168.1.1:9042", "192.168.1.2:9042"));
        replicasByDatacenter1.put("DC2", Arrays.asList("192.168.1.3:9042"));

        Map<String, List<String>> replicasByDatacenter2 = new HashMap<>();
        replicasByDatacenter2.put("DC1", Arrays.asList("192.168.1.2:9042"));
        replicasByDatacenter2.put("DC2", Arrays.asList("192.168.1.3:9042"));

        List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = Arrays.asList(
        new TokenRangeReplicasResponse.ReplicaInfo("-1000", "0", replicasByDatacenter1),
        new TokenRangeReplicasResponse.ReplicaInfo("0", "1000", replicasByDatacenter2)
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(replicaInfos, replicaInfos, metadata);

        // Test conversion
        RangeMap<BigInteger, List<CassandraInstance>> result =
        TokenRangeUtils.convertTokenRangeReplicasToRangeMap(response, instances, null);

        // Validate results
        assertThat(result).isNotNull();
        assertThat(result.asMapOfRanges()).hasSize(2);

        // Check first range
        List<CassandraInstance> replicas1 = result.get(new BigInteger("-500"));
        assertThat(replicas1).hasSize(3);
        assertThat(replicas1).extracting(CassandraInstance::nodeName)
                             .containsExactlyInAnyOrder("node1.example.com", "node2.example.com", "node3.example.com");

        // Check second range
        List<CassandraInstance> replicas2 = result.get(new BigInteger("500"));
        assertThat(replicas2).hasSize(2);
        assertThat(replicas2).extracting(CassandraInstance::nodeName)
                             .containsExactlyInAnyOrder("node2.example.com", "node3.example.com");
    }

    @Test
    void testConvertTokenRangeReplicasToRangeMapWithDatacenterFilter()
    {
        // Create test instances
        List<CassandraInstance> instances = Arrays.asList(
        new CassandraInstance("-1000", "node1.example.com", "DC1"),
        new CassandraInstance("0", "node2.example.com", "DC1"),
        new CassandraInstance("1000", "node3.example.com", "DC2")
        );

        // Create replica metadata
        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = new HashMap<>();
        metadata.put("192.168.1.1:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node1.example.com", "192.168.1.1", 9042, "DC1"));
        metadata.put("192.168.1.2:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node2.example.com", "192.168.1.2", 9042, "DC1"));
        metadata.put("192.168.1.3:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node3.example.com", "192.168.1.3", 9042, "DC2"));

        // Create replica info
        Map<String, List<String>> replicasByDatacenter = new HashMap<>();
        replicasByDatacenter.put("DC1", Arrays.asList("192.168.1.1:9042", "192.168.1.2:9042"));
        replicasByDatacenter.put("DC2", Arrays.asList("192.168.1.3:9042"));

        List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = Arrays.asList(
        new TokenRangeReplicasResponse.ReplicaInfo("-1000", "0", replicasByDatacenter)
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(replicaInfos, replicaInfos, metadata);

        // Test conversion with datacenter filter (include only DC1)
        RangeMap<BigInteger, List<CassandraInstance>> result =
        TokenRangeUtils.convertTokenRangeReplicasToRangeMap(response, instances, "DC1");

        // Validate results - should only contain DC1 replicas
        assertThat(result).isNotNull();
        assertThat(result.asMapOfRanges()).hasSize(1);

        List<CassandraInstance> replicas = result.get(new BigInteger("-500"));
        assertThat(replicas).hasSize(2);
        assertThat(replicas).extracting(CassandraInstance::nodeName)
                            .containsExactlyInAnyOrder("node1.example.com", "node2.example.com");
    }

    @Test
    void testConvertTokenRangeReplicasToRangeMapEmptyResponse()
    {
        List<CassandraInstance> instances = Arrays.asList(
        new CassandraInstance("0", "node1.example.com", "DC1")
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyMap()
        );

        RangeMap<BigInteger, List<CassandraInstance>> result =
        TokenRangeUtils.convertTokenRangeReplicasToRangeMap(response, instances, null);

        assertThat(result).isNotNull();
        assertThat(result.asMapOfRanges()).isEmpty();
    }

    @Test
    void testConvertTokenRangeReplicasToRangeMapWithUnknownFqdn()
    {
        // Create test instances
        List<CassandraInstance> instances = Arrays.asList(
        new CassandraInstance("-1000", "node1.example.com", "DC1")
        );

        // Create replica metadata with unknown FQDN
        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = new HashMap<>();
        metadata.put("192.168.1.1:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "unknown.example.com", "192.168.1.1", 9042, "DC1"));

        // Create replica info
        Map<String, List<String>> replicasByDatacenter = new HashMap<>();
        replicasByDatacenter.put("DC1", Arrays.asList("192.168.1.1:9042"));

        List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = Arrays.asList(
        new TokenRangeReplicasResponse.ReplicaInfo("-1000", "0", replicasByDatacenter)
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(replicaInfos, replicaInfos, metadata);

        // Test conversion
        RangeMap<BigInteger, List<CassandraInstance>> result =
        TokenRangeUtils.convertTokenRangeReplicasToRangeMap(response, instances, null);

        // Validate results - should have empty replica list since FQDN doesn't match
        assertThat(result).isNotNull();
        assertThat(result.asMapOfRanges()).hasSize(1);

        List<CassandraInstance> replicas = result.get(new BigInteger("-500"));
        assertThat(replicas).isEmpty();
    }

    @Test
    void testConvertTokenRangeReplicasToRangeMapWithBoundaryTokenValues()
    {
        // Create test instances
        List<CassandraInstance> instances = Arrays.asList(
        new CassandraInstance(String.valueOf(Long.MIN_VALUE), "nodeMin.example.com", "DC1"),
        new CassandraInstance("0", "nodeZero.example.com", "DC1"),
        new CassandraInstance(String.valueOf(Long.MAX_VALUE), "nodeMax.example.com", "DC2")
        );

        // Create replica metadata
        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = new HashMap<>();
        metadata.put("192.168.1.1:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "nodeMin.example.com", "192.168.1.1", 9042, "DC1"));
        metadata.put("192.168.1.2:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "nodeZero.example.com", "192.168.1.2", 9042, "DC1"));
        metadata.put("192.168.1.3:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "nodeMax.example.com", "192.168.1.3", 9042, "DC2"));

        // Create replica info with boundary values
        Map<String, List<String>> replicasByDatacenter = new HashMap<>();
        replicasByDatacenter.put("DC1", Arrays.asList("192.168.1.1:9042", "192.168.1.2:9042"));
        replicasByDatacenter.put("DC2", Arrays.asList("192.168.1.3:9042"));

        List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = Arrays.asList(
        new TokenRangeReplicasResponse.ReplicaInfo(String.valueOf(Long.MIN_VALUE), String.valueOf(Long.MAX_VALUE), replicasByDatacenter)
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(replicaInfos, replicaInfos, metadata);

        // Test conversion
        RangeMap<BigInteger, List<CassandraInstance>> result =
        TokenRangeUtils.convertTokenRangeReplicasToRangeMap(response, instances, null);

        // Validate results
        assertThat(result).isNotNull();
        assertThat(result.asMapOfRanges()).hasSize(1);

        List<CassandraInstance> replicas = result.get(BigInteger.ZERO);
        assertThat(replicas).hasSize(3);
        assertThat(replicas).extracting(CassandraInstance::nodeName)
                            .containsExactlyInAnyOrder("nodeMin.example.com", "nodeZero.example.com", "nodeMax.example.com");
    }

    @Test
    void testConvertTokenRangeReplicasToRangeMapFilterIncludeDatacenter()
    {
        // Create test instances across multiple datacenters
        List<CassandraInstance> instances = Arrays.asList(
        new CassandraInstance("-1000", "node1.example.com", "DC1"),
        new CassandraInstance("0", "node2.example.com", "DC1"),
        new CassandraInstance("1000", "node3.example.com", "DC2"),
        new CassandraInstance("2000", "node4.example.com", "DC3")
        );

        // Create replica metadata
        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = new HashMap<>();
        metadata.put("192.168.1.1:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node1.example.com", "192.168.1.1", 9042, "DC1"));
        metadata.put("192.168.1.2:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node2.example.com", "192.168.1.2", 9042, "DC1"));
        metadata.put("192.168.1.3:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node3.example.com", "192.168.1.3", 9042, "DC2"));
        metadata.put("192.168.1.4:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node4.example.com", "192.168.1.4", 9042, "DC3"));

        // Create replica info with replicas from all datacenters
        Map<String, List<String>> replicasByDatacenter = new HashMap<>();
        replicasByDatacenter.put("DC1", Arrays.asList("192.168.1.1:9042", "192.168.1.2:9042"));
        replicasByDatacenter.put("DC2", Arrays.asList("192.168.1.3:9042"));
        replicasByDatacenter.put("DC3", Arrays.asList("192.168.1.4:9042"));

        List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = Arrays.asList(
        new TokenRangeReplicasResponse.ReplicaInfo("-1000", "0", replicasByDatacenter)
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(replicaInfos, replicaInfos, metadata);

        // Test conversion with datacenter filter (include only DC2)
        RangeMap<BigInteger, List<CassandraInstance>> result =
        TokenRangeUtils.convertTokenRangeReplicasToRangeMap(response, instances, "DC2");

        // Validate results - should contain only DC2 replicas
        assertThat(result).isNotNull();
        assertThat(result.asMapOfRanges()).hasSize(1);

        List<CassandraInstance> replicas = result.get(new BigInteger("-500"));
        assertThat(replicas).hasSize(1);
        assertThat(replicas).extracting(CassandraInstance::nodeName)
                            .containsExactlyInAnyOrder("node3.example.com");
        assertThat(replicas).extracting(CassandraInstance::dataCenter)
                            .containsExactlyInAnyOrder("DC2");
    }

    @Test
    void testValidateTokenRangeReplicasResponseHappyPath()
    {
        // Create valid TokenRangeReplicasResponse
        List<CassandraInstance> instances = List.of(
        new CassandraInstance("-1000", "node1.example.com", "DC1")
        );

        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = new HashMap<>();
        metadata.put("192.168.1.1:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node1.example.com", "192.168.1.1", 9042, "DC1"));

        Map<String, List<String>> replicasByDatacenter = new HashMap<>();
        replicasByDatacenter.put("DC1", Arrays.asList("192.168.1.1:9042"));

        List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = Arrays.asList(
        new TokenRangeReplicasResponse.ReplicaInfo("-1000", "0", replicasByDatacenter)
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(replicaInfos, replicaInfos, metadata);

        // Should not throw any exception
        assertThat(response).isNotNull();
        TokenRangeUtils.validateTokenRangeReplicasResponse(response);
    }

    @Test
    void testValidateTokenRangeReplicasResponseNullResponse()
    {
        assertThatThrownBy(() -> TokenRangeUtils.validateTokenRangeReplicasResponse(null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Null TokenRangeReplicasResponse from sidecar");
    }

    @Test
    void testValidateTokenRangeReplicasResponseNullReplicaMetadata()
    {
        List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = Arrays.asList(
        new TokenRangeReplicasResponse.ReplicaInfo("-1000", "0", new HashMap<>())
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(replicaInfos, replicaInfos, null);

        assertThatThrownBy(() -> TokenRangeUtils.validateTokenRangeReplicasResponse(response))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Null replicaMetadata in TokenRangeReplicasResponse from sidecar");
    }

    @Test
    void testValidateTokenRangeReplicasResponseEmptyReplicaMetadata()
    {
        List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = Arrays.asList(
        new TokenRangeReplicasResponse.ReplicaInfo("-1000", "0", new HashMap<>())
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(replicaInfos, replicaInfos, Collections.emptyMap());

        assertThatThrownBy(() -> TokenRangeUtils.validateTokenRangeReplicasResponse(response))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Empty replicaMetadata in TokenRangeReplicasResponse from sidecar");
    }

    @Test
    void testValidateTokenRangeReplicasResponseNullFqdn()
    {
        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = new HashMap<>();
        metadata.put("192.168.1.1:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", null, "192.168.1.1", 9042, "DC1"));

        List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = Arrays.asList(
        new TokenRangeReplicasResponse.ReplicaInfo("-1000", "0", new HashMap<>())
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(replicaInfos, replicaInfos, metadata);

        assertThatThrownBy(() -> TokenRangeUtils.validateTokenRangeReplicasResponse(response))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("ReplicaMetadata entry '192.168.1.1:9042' has null or empty fqdn in TokenRangeReplicasResponse from sidecar");
    }

    @Test
    void testValidateTokenRangeReplicasResponseEmptyFqdn()
    {
        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = new HashMap<>();
        metadata.put("192.168.1.1:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "", "192.168.1.1", 9042, "DC1"));

        List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = Arrays.asList(
        new TokenRangeReplicasResponse.ReplicaInfo("-1000", "0", new HashMap<>())
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(replicaInfos, replicaInfos, metadata);

        assertThatThrownBy(() -> TokenRangeUtils.validateTokenRangeReplicasResponse(response))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("ReplicaMetadata entry '192.168.1.1:9042' has null or empty fqdn in TokenRangeReplicasResponse from sidecar");
    }

    @Test
    void testValidateTokenRangeReplicasResponseWhitespaceFqdn()
    {
        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = new HashMap<>();
        metadata.put("192.168.1.1:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "   ", "192.168.1.1", 9042, "DC1"));

        List<TokenRangeReplicasResponse.ReplicaInfo> replicaInfos = Arrays.asList(
        new TokenRangeReplicasResponse.ReplicaInfo("-1000", "0", new HashMap<>())
        );

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(replicaInfos, replicaInfos, metadata);

        assertThatThrownBy(() -> TokenRangeUtils.validateTokenRangeReplicasResponse(response))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("ReplicaMetadata entry '192.168.1.1:9042' has null or empty fqdn in TokenRangeReplicasResponse from sidecar");
    }

    @Test
    void testValidateTokenRangeReplicasResponseNullReadReplicas()
    {
        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = new HashMap<>();
        metadata.put("192.168.1.1:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node1.example.com", "192.168.1.1", 9042, "DC1"));

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(Collections.emptyList(), null, metadata);

        assertThatThrownBy(() -> TokenRangeUtils.validateTokenRangeReplicasResponse(response))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Null readReplicas in TokenRangeReplicasResponse from sidecar");
    }

    @Test
    void testValidateTokenRangeReplicasResponseEmptyReadReplicas()
    {
        Map<String, TokenRangeReplicasResponse.ReplicaMetadata> metadata = new HashMap<>();
        metadata.put("192.168.1.1:9042", new TokenRangeReplicasResponse.ReplicaMetadata(
        "NORMAL", "UP", "node1.example.com", "192.168.1.1", 9042, "DC1"));

        TokenRangeReplicasResponse response = new TokenRangeReplicasResponse(Collections.emptyList(), Collections.emptyList(), metadata);

        assertThatThrownBy(() -> TokenRangeUtils.validateTokenRangeReplicasResponse(response))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Empty readReplicas in TokenRangeReplicasResponse from sidecar");
    }
}
