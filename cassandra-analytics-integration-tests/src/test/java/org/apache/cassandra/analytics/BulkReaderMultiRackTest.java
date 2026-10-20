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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.spark.data.CassandraDataSourceHelper;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests bulk reads against a datacenter split across several racks.
 * <p>
 * This closes a coverage gap. Every other integration test places all nodes of a datacenter in a single rack, where
 * Cassandra's rack-aware replica assignment reduces to "take the next RF nodes in ring order" - exactly what the
 * reader derives locally. With one rack per replica, {@code NetworkTopologyStrategy} instead skips nodes to spread
 * replicas across racks (see {@code acceptableRackRepeats}), so a derived replica set can disagree with Cassandra's.
 * <p>
 * {@code CassandraRing} states that it assumes racks are not in use, so a divergence here would previously have been
 * invisible in CI.
 * <p>
 * The reads cannot tell the two topology sources apart: every replica here holds the data, so even a wrong replica
 * set returns every row. {@link #testCassandraSuppliedReplicasMatchCassandra()} therefore compares the reader's rings
 * with Cassandra's own placement directly. Reads from keyspaces where a wrong replica holds no data, that is with
 * witness replicas, are covered by the witness replica tests.
 */
class BulkReaderMultiRackTest extends SharedClusterSparkIntegrationTestBase
{
    static final List<String> DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
    private static final Pattern DESCRIBE_RING_RANGE = Pattern.compile("end_token:(-?\\d+), endpoints:\\[([^\\]]*)\\]");

    QualifiedName table = uniqueTestTableFullName(TEST_KEYSPACE);

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        // Four nodes, three racks, with the first two nodes sharing a rack, and RF 3.
        //
        // NetworkTopologyStrategy computes acceptableRackRepeats = RF - rackCount, which is 0 here, so a node whose
        // rack has already been used is skipped. Tokens are handed out in node order, so for the range whose first
        // replica is node 1 the reader derives the next three nodes in ring order, 1,2,3, while Cassandra must skip
        // node 2 (rack1 again) and pick 1,3,4. The two topology sources therefore genuinely disagree, which is the
        // situation no existing test creates.
        //
        // Four nodes is the minimum that produces a disagreement. With one node per rack, or with racks assigned
        // round-robin, rack-aware selection coincides with ring order and the test would prove nothing.
        return super.testClusterConfiguration()
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
                            default:
                                return dcAndRack("", "");
                        }
                    });
    }

    @Test
    void testReadWithDerivedRangesOnMultiRackCluster()
    {
        assertCompleteDataset(bulkReaderDataFrame(table).load());
    }

    @Test
    void testReadWithCassandraSuppliedRangesOnMultiRackCluster()
    {
        Dataset<Row> data = bulkReaderDataFrame(table, Collections.singletonMap("forcecassandratokenranges", "true"))
                            .load();
        assertCompleteDataset(data);
    }

    /**
     * Both topology sources return the same rows, as every replica holds the data; see
     * {@link #testCassandraSuppliedReplicasMatchCassandra()} for the check that tells them apart.
     */
    @Test
    void testBothTopologySourcesAgreeOnMultiRackCluster()
    {
        List<String> derived = sortedNames(bulkReaderDataFrame(table).load());
        List<String> supplied = sortedNames(bulkReaderDataFrame(table,
                                                                Collections.singletonMap("forcecassandratokenranges", "true"))
                                            .load());
        assertThat(supplied)
        .describedAs("derived and Cassandra-supplied ranges must return the same rows on a rack-aware cluster")
        .isEqualTo(derived);
        assertThat(derived).hasSize(DATASET.size());
    }

    @Test
    void testReadAtLocalQuorumOnMultiRackCluster()
    {
        Dataset<Row> data = bulkReaderDataFrame(table, Collections.singletonMap("forcecassandratokenranges", "true"))
                            .option("consistencyLevel", "LOCAL_QUORUM")
                            .load();
        assertCompleteDataset(data);
    }

    /**
     * The point of the test class. Taking replicas from Cassandra must reproduce Cassandra's rack-aware placement for
     * every range, while the local derivation, which ignores racks, must get at least one range wrong on this cluster.
     * The second assertion proves the cluster really exercises the difference, so the first is not trivially true.
     */
    @Test
    void testCassandraSuppliedReplicasMatchCassandra() throws UnknownHostException
    {
        Map<BigInteger, Set<String>> cassandraPlacement = cassandraReplicasByRangeEnd();
        assertThat(cassandraPlacement).isNotEmpty();
        CassandraRing supplied = readerRing(true);
        CassandraRing derived = readerRing(false);

        boolean derivedDiverges = false;
        for (Map.Entry<BigInteger, Set<String>> range : cassandraPlacement.entrySet())
        {
            assertThat(replicaNames(supplied, range.getKey()))
            .describedAs("Cassandra-supplied replicas of the range ending at %s", range.getKey())
            .isEqualTo(range.getValue());
            derivedDiverges |= !replicaNames(derived, range.getKey()).equals(range.getValue());
        }
        assertThat(derivedDiverges)
        .describedAs("locally derived replicas should differ from Cassandra's for some range on a rack-aware cluster, "
                     + "otherwise this cluster does not exercise rack-aware placement")
        .isTrue();
    }

    /**
     * Builds the ring a bulk read would use, without taking a snapshot
     */
    private CassandraRing readerRing(boolean forceCassandraTokenRanges)
    {
        getOrCreateSparkSession();
        Map<String, String> options = new HashMap<>(mtlsTestHelper.mtlOptionMap());
        options.put("sidecar_contact_points", SparkTestUtils.sidecarInstancesOptionStream(cluster, dnsResolver)
                                                            .collect(Collectors.joining(",")));
        options.put("sidecar_port", String.valueOf(server.actualPort()));
        options.put("keyspace", table.keyspace());
        options.put("table", table.table());
        options.put("DC", "datacenter1");
        options.put("snapshotName", UUID.randomUUID().toString());
        options.put("createSnapshot", "false");
        options.put("clearSnapshotStrategy", "noop");
        options.put("forcecassandratokenranges", String.valueOf(forceCassandraTokenRanges));
        CassandraDataLayer dataLayer = (CassandraDataLayer) CassandraDataSourceHelper.getDataLayer(options, CassandraDataLayer::initialize);
        assertThat(dataLayer.ring().hasExplicitRanges()).isEqualTo(forceCassandraTokenRanges);
        return dataLayer.ring();
    }

    private static Set<String> replicaNames(CassandraRing ring, BigInteger token)
    {
        return ring.getReplicas(token).stream().map(CassandraInstance::nodeName).collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Cassandra's own placement, from {@code nodetool describering}, keyed by the inclusive end of each range
     */
    private Map<BigInteger, Set<String>> cassandraReplicasByRangeEnd() throws UnknownHostException
    {
        NodeToolResult result = cluster.getFirstRunningInstance().nodetoolResult("describering", table.keyspace());
        result.asserts().success();
        Map<BigInteger, Set<String>> placement = new LinkedHashMap<>();
        Matcher matcher = DESCRIBE_RING_RANGE.matcher(result.getStdout());
        while (matcher.find())
        {
            Set<String> names = new TreeSet<>();
            for (String endpoint : matcher.group(2).split(","))
            {
                String address = endpoint.trim().replaceFirst("^/", "").replaceFirst(":\\d+$", "");
                names.add(dnsResolver.reverseResolve(address));
            }
            placement.put(new BigInteger(matcher.group(1)), names);
        }
        return placement;
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
