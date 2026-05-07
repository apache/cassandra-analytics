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

package org.apache.cassandra.analytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.spark.data.PartitionedDataLayer;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.NotEnoughReplicasException;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3_DC2_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

public class BulkReaderMultiDCConsistencyTest extends SharedClusterSparkIntegrationTestBase
{
    static final List<String> OG_DATASET = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
    static final int TEST_KEY = 1;
    static final String TEST_VAL = "C*";
    QualifiedName table1 = uniqueTestTableFullName(TEST_KEYSPACE);

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .dcCount(2)
                    .nodesPerDc(3)
                    .dcAndRackSupplier((nodeId) -> {
                        switch (nodeId)
                        {
                            case 1:
                            case 2:
                            case 3:
                                return dcAndRack("datacenter1", "rack1");
                            case 4:
                            case 5:
                            case 6:
                                return dcAndRack("datacenter2", "rack1");
                            default:
                                return dcAndRack("", "");
                        }
                    });
    }

    /**
     * Happy path test. All nodes have the updated values.
     * QUORUM == EACH_QUORUM == ALL == driver read
     */
    @Test
    void happyPathTest()
    {
        List<String> testDataSet = new ArrayList<>(OG_DATASET);
        testDataSet.set(TEST_KEY, TEST_VAL);

        // Set value=TEST_VAL for key=TEST_KEY for all nodes
        setValueForALL(TEST_KEY, TEST_VAL);

        // Bulk read with ALL consistency
        List<Row> rowList = bulkRead(ConsistencyLevel.ALL.name());
        validateBulkReadRows(rowList, testDataSet);

        // Bulk read with QUORUM consistency
        rowList = bulkRead(ConsistencyLevel.QUORUM.name());
        validateBulkReadRows(rowList, testDataSet);

        // Bulk read with EACH_QUORUM consistency
        rowList = bulkRead(ConsistencyLevel.EACH_QUORUM.name());
        validateBulkReadRows(rowList, testDataSet);

        // Read the value for the test key using driver for different consistency levels
        String valAll = readValueForKey(TEST_KEY, ConsistencyLevel.ALL);
        String valQuorum = readValueForKey(TEST_KEY, ConsistencyLevel.QUORUM);
        String valEachQuorum = readValueForKey(TEST_KEY, ConsistencyLevel.EACH_QUORUM);
        assertThat(valAll).isEqualTo(valQuorum).isEqualTo(valEachQuorum).isEqualTo(rowList.get(1).getString(1));

        // Revert the value update for all nodes
        setValueForALL(TEST_KEY, OG_DATASET.get(TEST_KEY));
    }

    public static PartitionedDataLayer.AvailabilityHint getAvailability(CassandraInstance instance)
    {
        if (instance.nodeName().equals("localhost5") || instance.nodeName().equals("localhost6"))
        {
            return PartitionedDataLayer.AvailabilityHint.MOVING;
        }
        return PartitionedDataLayer.AvailabilityHint.UP;
    }

    /**
     * This test creates a scenario where bulk reader reads the most recently updated value with EACH_QUORUM
     * but reads stale value with QUORUM. This shows that QUORUM is different from EACH_QUORUM in multi-dc settings.
     * Here node5(DC2) and node6(DC2) has the update value for TEST_KEY.
     *
     * @throws NoSuchMethodException
     */
    @Test
    void eachQuorumIsNotQuorum() throws IOException, NoSuchMethodException
    {
        // The agent must be installed before ClassReloadingStrategy.fromInstalledAgent() — otherwise
        // it throws IllegalStateException when the JVM hosting this test class was started without a
        // prior ByteBuddy install (e.g. when the CI harness runs this class in its own gradle invocation).
        ByteBuddyAgent.install();
        // Hold on to the original strategy to reset ByteBuddy after this test
        ClassReloadingStrategy crStrategy = ClassReloadingStrategy.fromInstalledAgent();

        // We need the try/finally structure since this test mutates global state through the ByteBuddy changes; if we
        // time out during the test run that change persists and, pending test ordering, will cascade and take the rest
        // with it.
        try
        {
            List<String> updatedDataSet = new ArrayList<>(OG_DATASET);
            updatedDataSet.set(1, TEST_VAL);

            // Internally update value for TEST_KEY for node5 and node6. This update doesn't propagate to other nodes.
            updateValueNodeInternal(5, TEST_KEY, TEST_VAL);
            updateValueNodeInternal(6, TEST_KEY, TEST_VAL);

            // Bytecode injection to simulate a scenario where node5 and node6 are at the end of the replica list for bulk reader.
            // This simulation mimics a real world scenario.
            // With this arrangement PartitionedDataLayer.splitReplicas method for QUORUM will split the replicas like below:
            // primaryReplicas: [Node1, Node2, Node3, Node4]
            // secondaryReplicas: [Node5, Node6]
            // Number of nodes required for QUORUM read id 6/1 + 1 = 4. Bulk reader will read from [Node1, Node2, Node3, Node4] only.
            new ByteBuddy()
                    .redefine(CassandraDataLayer.class)
                    .method(ElementMatchers.named("getAvailability"))
                    .intercept(
                            MethodCall.invoke(BulkReaderMultiDCConsistencyTest.class.getMethod("getAvailability", CassandraInstance.class))
                                    .withAllArguments()
                    )
                    .make()
                    .load(CassandraDataLayer.class.getClassLoader(), crStrategy);

            // Bulk read with QUORUM consistency
            List<Row> rowList = bulkRead(ConsistencyLevel.QUORUM.name());
            // Validate that the result doesn't have the updated data.
            validateBulkReadRows(rowList, OG_DATASET);

            // Message filter to mimic message drops from Node5 and Node6 to Node1.
            // We are setting this up to simulate a scenario where reading values with QUORUM consistency with driver
            // and using Node1 as the coordinator doesn't get the values from Node5 and Node6.
            cluster.filters().allVerbs().from(5).to(1).drop();
            cluster.filters().allVerbs().from(6).to(1).drop();

            // Read value for TEST_KEY with driver using Node1 as coordinator.
            // The message filters above drop responses from Node5/Node6 to Node1, but the coordinator's
            // snitch-based replica selection may still pick Node5 or Node6 as one of its 4 QUORUM replicas.
            // When that happens the dropped response causes a ReadTimeoutException. This is infrastructure
            // noise from the message filter and not a real test failure, which would manifest as an
            // AssertionError (wrong value returned), not a timeout. Retry to tolerate the non-deterministic
            // replica selection. This test was _very_ intermittently flaky before, so if we take something that
            // flaked out 1% of the time for instance and then repeat 10x, we _should_ be in a better place.
            // Another option here would be to just keep spinning on ReadTimeoutExceptions until the junit
            // timeout timer but that just seems excessive.
            String quorumVal = null;
            for (int attempt = 1; attempt <= 10; attempt++)
            {
                try
                {
                    quorumVal = readValueForKey(cluster.get(1).coordinator(), TEST_KEY, ConsistencyLevel.QUORUM);
                    break;
                }
                catch (Exception e)
                {
                    // ReadTimeoutException here is of type org.apache.cassandra.exceptions.ReadTimeoutException
                    // which is not available on the integration-tests compile classpath
                    // Hence checking for class name instead of using instanceof
                    if (attempt == 10 || !e.getClass().getName().endsWith("ReadTimeoutException"))
                    {
                        throw e;
                    }
                }
            }
            assertThat(quorumVal).isEqualTo(OG_DATASET.get(TEST_KEY));

            // Bulk read with EACH_QUORUM consistency
            rowList = bulkRead(ConsistencyLevel.EACH_QUORUM.name());
            // Validate that bulk reader was able to read the updated value
            validateBulkReadRows(rowList, updatedDataSet);

            // Read value using driver with EACH_QUORUM. Must use a DC2 coordinator (node4) because the active message
            // filters drop responses from nodes 5 and 6 back to node1 (DC1), making EACH_QUORUM unsatisfiable from
            // node1's perspective. We used to reset our ByteBuddy filters mid-test but timeouts on the test would
            // then cascade and cause all other tests to fail. Now we need to have this structured workaround
            // to respect the message filters we have in place but still confirm the data is where we expect it
            // and that the client version matches bulk's view of the world.
            String eachQuorumVal = readValueForKey(cluster.get(4).coordinator(), TEST_KEY, ConsistencyLevel.EACH_QUORUM);
            // Validate that EACH_QUORUM read using driver and the bulk reader are the same
            assertThat(eachQuorumVal).isEqualTo(rowList.get(TEST_KEY).getString(1));
        }
        finally
        {
            // Reset message filters and data unconditionally so a mid-test failure doesn't leave dirty
            // state that corrupts subsequent tests (e.g. eachQuorumFailureWithTwoNodesDownOneDC).
            cluster.filters().reset();
            setValueForALL(TEST_KEY, OG_DATASET.get(TEST_KEY));
            crStrategy.reset(CassandraDataLayer.class);
        }
    }

    /**
     * Tests that EACH_QUORUM read succeeds with one node down in each DC.
     * Tests that value read using driver is the same as the value read using bulk reader.
     *
     * As this is a topology destructive test, we tear down and recreate the cluster.
     *
     * @throws Exception
     */
    @Test
    void eachQuorumSuccessWithOneNodeDownEachDC() throws Exception
    {
        try
        {
            // Stop Node1(DC1)
            cluster.stopUnchecked(cluster.get(1));
            // Stop Node4(DC2)
            cluster.stopUnchecked(cluster.get(4));

            // Bulk read with EACH_QUORUM consistency
            List<Row> rowList = bulkRead(ConsistencyLevel.EACH_QUORUM.name());
            validateBulkReadRows(rowList, OG_DATASET);

            // Read TEST_KEY using driver
            String eachQuorumVal = readValueForKey(TEST_KEY, ConsistencyLevel.EACH_QUORUM);
            // Validate that data from driver and bulk reader are the same
            assertThat(eachQuorumVal).isEqualTo(rowList.get(TEST_KEY).getString(1));
        }
        finally
        {
            // Tear down and re-create the cluster
            tearDown();
            setup();
        }
    }

    /**
     * Tests that:
     * QUORUM read succeeds with two nodes down in a single DC.
     * QUORUM read value using bulk reader equals QUORUM read value using driver.
     * EACH_QUORUM read with bulk reader fails with cause as NotEnoughReplicasException.
     * EACH_QUORUM read with driver fails.
     *
     * As this is a topology destructive test, we tear down and recreate the cluster.
     *
     * @throws Exception
     */
    @Test
    void eachQuorumFailureWithTwoNodesDownOneDC() throws Exception
    {
        try
        {
            // Stop Node4(DC2)
            cluster.stopUnchecked(cluster.get(4));
            // Stop Node5(DC2)
            cluster.stopUnchecked(cluster.get(5));

            // Bulk read with QUORUM
            List<Row> rowList = bulkRead(ConsistencyLevel.QUORUM.name());
            validateBulkReadRows(rowList, OG_DATASET);
            // Driver read with QUORUM
            String quorumVal = readValueForKey(TEST_KEY, ConsistencyLevel.QUORUM);
            // Bulk read and driver read values are the same
            assertThat(quorumVal).isEqualTo(rowList.get(TEST_KEY).getString(1));

            // Try bulk reading with EACH_QUORUM consistency. Assert that it fails with the correct cause.
            try
            {
                bulkRead(ConsistencyLevel.EACH_QUORUM.name());
            }
            catch (Exception ex)
            {
                assertThat(ex).isNotNull();
                assertThat(ex).isInstanceOf(SparkException.class);
                assertThat(ex.getCause()).isInstanceOf(NotEnoughReplicasException.class);
                assertThat(ex.getCause().getMessage()).isEqualTo("Required 2 replicas but only 1 responded");
            }

            // Try driver reading with EACH_QUORUM consistency. Assert that it fails with the correct error.
            try
            {
                readValueForKey(TEST_KEY, ConsistencyLevel.EACH_QUORUM);
            }
            catch (Exception ex)
            {
                assertThat(ex).isNotNull();
                assertThat(ex.getMessage()).isEqualTo("Cannot achieve consistency level EACH_QUORUM in DC datacenter2");
            }
        }
        finally
        {
            // Tear down and re-create the cluster
            tearDown();
            setup();
        }
    }

    /**
     * Validates that read repair is disabled.
     */
    private void validateReadRepairIsDisabled()
    {
        // Update value for Node1 only
        updateValueNodeInternal(1, TEST_KEY, TEST_VAL);
        // Validate only Node1 has the updated value
        validateNodeInternalValue(1, TEST_KEY, TEST_VAL);
        validateNodeInternalValue(2, TEST_KEY, OG_DATASET.get(1));
        validateNodeInternalValue(5, TEST_KEY, OG_DATASET.get(1));

        // Read with ALL consistency using coordinator.
        // If read repair is enabled this should update the value for all nodes.
        readValueForKey(TEST_KEY, ConsistencyLevel.ALL);

        // Validate only Node1 has the updated value
        validateNodeInternalValue(1, TEST_KEY, TEST_VAL);
        validateNodeInternalValue(2, TEST_KEY, OG_DATASET.get(1));
        validateNodeInternalValue(5, TEST_KEY, OG_DATASET.get(1));

        // Revert the value update for all nodes
        setValueForALL(TEST_KEY, OG_DATASET.get(TEST_KEY));
    }

    @NotNull
    private List<Row> bulkRead(String consistency)
    {
        List<Row> rowList;
        Dataset<Row> dataForTable1;
        dataForTable1 = bulkReaderDataFrame(table1)
                        .option("consistencyLevel", consistency)
                        .option("dc", null)
                        .option("maxRetries", 1)
                        .option("maxMillisToSleep", 50)
                        .option("defaultMillisToSleep", 50)
                        .load();

        rowList = dataForTable1.collectAsList().stream()
                               .sorted(Comparator.comparing(row -> row.getInt(0)))
                               .collect(Collectors.toList());
        return rowList;
    }

    private static void validateBulkReadRows(List<Row> rowList, List<String> dataSet)
    {
        for (int i = 0; i < dataSet.size(); i++)
        {
            assertThat(rowList.get(i).getInt(0)).isEqualTo(i);
            assertThat(rowList.get(i).getString(1)).isEqualTo(dataSet.get(i));
        }
    }

    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3_DC2_RF3);
        // Read repair disabled: https://cassandra.apache.org/doc/latest/cassandra/managing/operating/read_repair.html?utm_source=chatgpt.com#none
        createTestTable(table1, "CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, name text) with read_repair='NONE';");

        IInstance firstRunningInstance = cluster.getFirstRunningInstance();
        for (int i = 0; i < OG_DATASET.size(); i++)
        {
            String value = OG_DATASET.get(i);
            String query1 = String.format("INSERT INTO %s (id, name) VALUES (%d, '%s');", table1, i, value);

            firstRunningInstance.coordinator().execute(query1, ConsistencyLevel.ALL);
        }
        validateReadRepairIsDisabled();
    }

    private void updateValueNodeInternal(int node, int key, String value)
    {
        cluster.get(node).executeInternal(String.format("UPDATE %s SET name='%s' WHERE id=%d", table1, value, key));
    }

    private void validateNodeInternalValue(int node, int key, String val)
    {
        assertThat(getNodeInternalValue(node, key)).isEqualTo(val);
    }

    private String getNodeInternalValue(int node, int key)
    {
        Object[][] result = cluster.get(node)
                                   .executeInternal(String.format("SELECT name FROM %s WHERE id=%d", table1, key));
        return (String) result[0][0];
    }

    private String readValueForKey(int key, ConsistencyLevel consistencyLevel)
    {
        return readValueForKey(cluster.getFirstRunningInstance().coordinator(), key, consistencyLevel);
    }

    private String readValueForKey(ICoordinator coordinator, int key, ConsistencyLevel consistencyLevel)
    {
        Object[][] result = coordinator
                            .execute(String.format("SELECT name FROM %s WHERE id=%d", table1, key), consistencyLevel);
        return (String) result[0][0];
    }

    /**
     * Sets value for a key with consistency level ALL.
     *
     * @param key
     * @param value
     */
    private void setValueForALL(int key, String value)
    {
        cluster.getFirstRunningInstance()
               .coordinator()
               .execute(String.format("UPDATE %s SET name='%s' WHERE id=%d", table1, value, key), ConsistencyLevel.ALL);
    }
}
