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

import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3_DC2_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks at the data level that a bulk read uses the replicas Cassandra chose, on a cluster where rack-aware placement
 * differs from the reader's local derivation.
 * <p>
 * Two datacenters of four nodes, each with uneven racks:
 * <pre>
 * datacenter1: rack1 [node1, node2], rack2 [node3], rack3 [node4]
 * datacenter2: rack1 [node5, node6], rack2 [node7], rack3 [node8]
 * </pre>
 * With RF 3 per datacenter, {@code NetworkTopologyStrategy} spreads each range's replicas across racks, so it skips the
 * second node of rack1 where the derivation, which ignores racks, would take it. The test finds a row with a replica
 * that only rack-aware placement includes, gives that replica alone a newer value, and reads at ALL: a read from
 * Cassandra's replicas returns the newer value, while a read from the derived replicas misses it.
 */
class BulkReaderRackAwareReplicaSelectionTest extends SharedClusterSparkIntegrationTestBase
{
    private static final int ROW_COUNT = 100;
    private static final String UPDATED = "updated";

    QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE);

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .dcCount(2)
                    .nodesPerDc(4)
                    .dcAndRackSupplier((nodeId) -> {
                        switch (nodeId)
                        {
                            case 1:
                            case 2:
                                return dcAndRack("datacenter1", "rack1");
                            case 3:
                                return dcAndRack("datacenter1", "rack2");
                            case 4:
                                return dcAndRack("datacenter1", "rack3");
                            case 5:
                            case 6:
                                return dcAndRack("datacenter2", "rack1");
                            case 7:
                                return dcAndRack("datacenter2", "rack2");
                            case 8:
                                return dcAndRack("datacenter2", "rack3");
                            default:
                                return dcAndRack("", "");
                        }
                    });
    }

    @Test
    void testBulkReadUsesTheReplicasCassandraChose() throws UnknownHostException
    {
        Map<BigInteger, Set<String>> placement = cassandraReplicasByRangeEnd(table.keyspace());
        CassandraRing derived = bulkReaderRing(table, true);
        Set<String> datacenter1 = derived.instances().stream()
                                         .map(CassandraInstance::nodeName)
                                         .collect(Collectors.toCollection(TreeSet::new));

        // Find a row with a datacenter1 replica that the derivation leaves out
        Integer id = null;
        String rackAwareOnlyReplica = null;
        Object[][] rows = cluster.getFirstRunningInstance()
                                 .coordinator()
                                 .execute(String.format("SELECT id, token(id) FROM %s", table), ConsistencyLevel.ALL);
        for (Object[] row : rows)
        {
            BigInteger token = BigInteger.valueOf((Long) row[1]);
            Set<String> cassandraReplicas = new TreeSet<>(replicasOf(placement, token));
            cassandraReplicas.retainAll(datacenter1);
            Set<String> derivedReplicas = derived.getReplicas(token).stream()
                                                 .map(CassandraInstance::nodeName)
                                                 .collect(Collectors.toSet());
            cassandraReplicas.removeAll(derivedReplicas);
            if (!cassandraReplicas.isEmpty())
            {
                id = (Integer) row[0];
                rackAwareOnlyReplica = cassandraReplicas.iterator().next();
                break;
            }
        }
        assertThat(rackAwareOnlyReplica)
        .describedAs("some row should have a replica only rack-aware placement includes, "
                     + "otherwise this cluster does not exercise rack-aware placement")
        .isNotNull();

        // Give that replica alone a newer value
        instanceNamed(rackAwareOnlyReplica).executeInternal(String.format("UPDATE %s SET name = ? WHERE id = ?", table),
                                                            UPDATED, id);

        assertThat(bulkReadName(id, false))
        .describedAs("a bulk read from Cassandra's replicas includes %s and returns its newer value", rackAwareOnlyReplica)
        .isEqualTo(UPDATED);
        assertThat(bulkReadName(id, true))
        .describedAs("a bulk read from the derived replicas leaves %s out, which is the bug this guards against",
                     rackAwareOnlyReplica)
        .isEqualTo(originalName(id));

        // Last, as a read at ALL repairs the other replicas
        Object[][] driverRead = cluster.getFirstRunningInstance()
                                       .coordinator()
                                       .execute(String.format("SELECT name FROM %s WHERE id = ?", table), ConsistencyLevel.ALL, id);
        assertThat(driverRead[0][0]).isEqualTo(UPDATED);
    }

    /**
     * The replicas of the range containing {@code token}: the range whose inclusive end is the first at or after the
     * token, wrapping around to the first range
     */
    private static Set<String> replicasOf(Map<BigInteger, Set<String>> placement, BigInteger token)
    {
        List<BigInteger> ends = placement.keySet().stream().sorted().collect(Collectors.toList());
        BigInteger end = ends.stream().filter(candidate -> candidate.compareTo(token) >= 0).findFirst().orElse(ends.get(0));
        return placement.get(end);
    }

    private IInstance instanceNamed(String nodeName) throws UnknownHostException
    {
        for (IInstance instance : cluster)
        {
            if (dnsResolver.reverseResolve(instance.broadcastAddress().getAddress().getHostAddress()).equals(nodeName))
            {
                return instance;
            }
        }
        throw new IllegalArgumentException("No instance named " + nodeName);
    }

    private String bulkReadName(int id, boolean deriveTokenRanges)
    {
        Map<String, String> options = deriveTokenRanges
                                      ? Collections.singletonMap("derivetokenranges", "true")
                                      : Collections.emptyMap();
        List<Row> rows = bulkReaderDataFrame(table, options).option("consistencyLevel", "ALL")
                                                            .option("dc", null)
                                                            .load()
                                                            .filter("id = " + id)
                                                            .collectAsList();
        assertThat(rows).hasSize(1);
        return rows.get(0).getString(1);
    }

    private static String originalName(int id)
    {
        return "value_" + id;
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3_DC2_RF3);
        createTestTable(table, "CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, name text);");

        IInstance firstRunningInstance = cluster.getFirstRunningInstance();
        for (int id = 0; id < ROW_COUNT; id++)
        {
            firstRunningInstance.coordinator()
                                .execute(String.format("INSERT INTO %s (id, name) VALUES (%d, '%s');",
                                                       table, id, originalName(id)),
                                         ConsistencyLevel.ALL);
        }
    }
}
