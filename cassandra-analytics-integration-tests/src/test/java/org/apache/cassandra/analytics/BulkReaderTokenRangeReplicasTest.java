package org.apache.cassandra.analytics;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.data.partitioner.MurmurHash;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.utils.ClusterUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3_DC2_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * <h3>Single rack replica placement with NetworkTopologyStrategy</h3>
 * <ul>
 *   <li>DC1: {Rack1:[Node1, Node2, Node3, Node4]}, Replication Factor: 3</li>
 *   <li>DC2: {Rack1:[Node5, Node6, Node7, Node8]}, Replication Factor: 3</li>
 * </ul>
 *
 * <p>Token range ownership: {Node1: T1, Node2: T2, Node3: T3, Node4: T4, Node5: T5, Node6: T6, Node7: T7, Node8: T8}</p>
 * <p>T1 will be replicated in the next 2 nodes in the same DC1(Node2, Node3) and the first 3 nodes in DC2(Node5, Node6, Node7).</p>
 * <p>For each token range the replicas are:</p>
 * <pre>
 * T1:[Node1, Node2, Node3, Node5, Node6, Node7]
 * T2:[Node2, Node3, Node4, Node5, Node6, Node7]
 * T3:[Node3, Node4, Node1, Node5, Node6, Node7]
 * T4:[Node4, Node1, Node2, Node5, Node6, Node7]
 * T5:[Node5, Node6, Node7, Node1, Node2, Node3]
 * T6:[Node6, Node7, Node8, Node1, Node2, Node3]
 * T7:[Node7, Node8, Node5, Node1, Node2, Node3]
 * T8:[Node8, Node5, Node6, Node1, Node2, Node3]
 * </pre>
 *
 * <h3>Multi-rack replica placement with NetworkTopologyStrategy</h3>
 * <ul>
 *   <li>DC1: {Rack1:[Node1, Node2], Rack2:[Node3], Rack3:[Node4]}, Replication Factor: 3</li>
 *   <li>DC2: {Rack1:[Node5, Node6], Rack2:[Node7], Rack3:[Node8]}, Replication Factor: 3</li>
 * </ul>
 *
 * <p>Cassandra will try to place replicas in different racks.</p>
 * <p>T1 will be replicated in the next 2 nodes in the same DC1 and different racks (Node3, Node4) and the first 3 nodes in different racks in DC2(Node5, Node7, Node8).</p>
 * <p>For each token range the replicas are:</p>
 * <pre>
 * T1:[Node1, Node3, Node4, Node5, Node7, Node8]
 * T2:[Node2, Node3, Node4, Node5, Node7, Node8]
 * T3:[Node3, Node4, Node1, Node5, Node7, Node8]
 * T4:[Node4, Node1, Node3, Node5, Node7, Node8]
 * T5:[Node5, Node7, Node8, Node1, Node3, Node4]
 * T6:[Node6, Node7, Node8, Node1, Node3, Node4]
 * T7:[Node7, Node8, Node5, Node1, Node3, Node4]
 * T8:[Node8, Node5, Node7, Node1, Node3, Node4]
 * </pre>
 */
public class BulkReaderTokenRangeReplicasTest extends SharedClusterSparkIntegrationTestBase
{
    QualifiedName table1 = uniqueTestTableFullName(TEST_KEYSPACE);
    private static final String VALUE1 = "VAL1";
    private static final String VALUE2 = "VAL2";

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
                        }
                        return dcAndRack("", "");
                    });
    }


    @Test
    void testMultiDCMultiRack()
    {
        // get token for node 1
        long token = getTokenForNode(1);
        // reverse hash the token to a blob key
        ByteBuffer key = keyForToken(token);
        // insert value for the key in node 1 token range
        insert(key, VALUE1);
        // Nodes placement:
        // DC1: {rack1: [node1, node2], rack2:[node3], rack3:[node4]}
        // DC2: {rack1: [node5, node6], rack2:[node7], rack3:[node8]}
        // validate that all nodes except node 2, 6 stored the key, value.
        Map<Integer, String> expectedValuesInNodes = new HashMap<>(Map.of(1, VALUE1,
                                                                          3, VALUE1,
                                                                          4, VALUE1,
                                                                          5, VALUE1,
                                                                          7, VALUE1,
                                                                          8, VALUE1));
        validateValuesInNodes(expectedValuesInNodes, key);

        // update the value internally at node 4
        updateInternal(4, key, VALUE2);
        // validate the values across the nodes:
        // node 4 should have VALUE2
        // node 2, node 6 shouldn't have the key
        // all other nodes should have VALUE1
        expectedValuesInNodes.put(4, VALUE2);
        validateValuesInNodes(expectedValuesInNodes, key);

        List<Row> rowList = bulkRead(ConsistencyLevel.ALL.name());
        Object[][] driverVal = readFromCluster(key, ConsistencyLevel.ALL);

        // validate that the value matches the data read using driver
        assertThat(rowList).isNotNull();
        assertThat(rowList).isNotEmpty();
        assertThat(driverVal).isNotNull();
        assertThat(driverVal).isNotEmpty();
        assertThat(driverVal[0][0]).isInstanceOf(String.class);
        assertThat(rowList.get(0).getString(1)).isEqualTo((String) driverVal[0][0]);
    }

    private void validateValuesInNodes(Map<Integer, String> values, ByteBuffer key)
    {
        for (int i = 1; i <= 8; i++)
        {
            Object[][] obj = getInternal(i, key);
            if (!values.containsKey(i))
            {
                assertThat(obj).isEmpty();
            }
            else
            {
                assertThat(obj).isNotEmpty();
                assertThat(obj[0][0]).isInstanceOf(String.class);
                assertThat((String) obj[0][0]).isEqualTo(values.get(i));
            }
        }
    }

    @NotNull
    private List<Row> bulkRead(String consistency)
    {
        List<Row> rowList;
        Dataset<Row> dataForTable1;
        dataForTable1 = bulkReaderDataFrame(table1)
                        .option("consistencyLevel", consistency)
                        .option("dc", null)
                        .load();

        rowList = dataForTable1.collectAsList().stream()
                               .sorted(Comparator.comparing(row -> row.getInt(0)))
                               .collect(Collectors.toList());
        return rowList;
    }

    private Object[][] getInternal(int node, ByteBuffer key)
    {
        return cluster.get(node).executeInternal(String.format("SELECT value FROM %s WHERE key = ?", table1), key);
    }

    private Object[][] readFromCluster(ByteBuffer key, ConsistencyLevel consistencyLevel)
    {
        return cluster.getFirstRunningInstance().coordinator().execute(String.format("SELECT value FROM %s WHERE key = ?", table1), consistencyLevel, key);
    }

    private void insert(ByteBuffer key, String value)
    {
        String query1 = String.format("INSERT INTO %s (key, value) VALUES (?, ?);", table1);
        cluster.getFirstRunningInstance().coordinator().execute(query1, ConsistencyLevel.ALL, key, value);
    }

    private void updateInternal(int node, ByteBuffer key, String value)
    {
        cluster.get(node).executeInternal(String.format("UPDATE %s SET value='%s' WHERE key=?", table1, value), key);
    }

    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3_DC2_RF3);
        createTestTable(table1, "CREATE TABLE IF NOT EXISTS %s (key blob, value text, PRIMARY KEY (key));");
    }

    private long getTokenForNode(int nodeNumber)
    {
        String nodeAddress = cluster.get(nodeNumber).config().broadcastAddress().getAddress().getHostAddress();
        List<ClusterUtils.RingInstanceDetails> ringDetails = ClusterUtils.ring(cluster.get(nodeNumber));

        return ringDetails.stream()
                          .filter(details -> details.getAddress().contains(nodeAddress))
                          .findFirst()
                          .map(details -> Long.parseLong(details.getToken()))
                          .orElseThrow(() -> new RuntimeException("Node " + nodeNumber + " token not found"));
    }

    public static ByteBuffer keyForToken(long token)
    {
        ByteBuffer result = ByteBuffer.allocate(16);
        long[] inv = MurmurHash.inv_hash3_x64_128(new long[]{ token, 0L });
        result.putLong(inv[0]).putLong(inv[1]).position(0);
        return result;
    }
}
