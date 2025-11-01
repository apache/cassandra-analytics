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

import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.data.partitioner.JDKSerializationTests;
import org.apache.cassandra.spark.data.partitioner.MultipleReplicasTests;
import org.apache.cassandra.spark.data.partitioner.NotEnoughReplicasException;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.reader.EmptyStreamScanner;
import org.apache.cassandra.spark.reader.StreamScanner;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.TaskContext;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.AVAILABILITY_HINT_COMPARATOR;
import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.DOWN;
import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.JOINING;
import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.LEAVING;
import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.MOVING;
import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.UNKNOWN;
import static org.apache.cassandra.spark.data.PartitionedDataLayer.AvailabilityHint.UP;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.ALL;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.ANY;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.ONE;
import static org.apache.cassandra.spark.data.partitioner.ConsistencyLevel.TWO;
import static org.apache.cassandra.spark.data.partitioner.Partitioner.Murmur3Partitioner;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.Generate.pick;

public class PartitionedDataLayerTests extends VersionRunner
{
    int partitionId;

    @BeforeEach
    public void setup()
    {
        partitionId = TaskContext.getPartitionId();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSplitQuorumAllUp(CassandraBridge bridge)
    {
        runSplitTests(1, UP);
        runSplitTests(2, UP, UP);
        runSplitTests(2, UP, UP, UP);
        runSplitTests(3, UP, UP, UP, UP, UP);
    }

    @Test
    public void testSplitQuorumOneDown()
    {
        runSplitTests(1, DOWN);
        runSplitTests(2, DOWN, UP);
        runSplitTests(2, DOWN, UP, UP);
        runSplitTests(3, UP, DOWN, UP, UP, UP);
    }

    @Test
    public void testSplitQuorumOneLeavingOrMoving()
    {
        runSplitTests(1, LEAVING);
        runSplitTests(2, LEAVING, DOWN);
        runSplitTests(2, DOWN, LEAVING, MOVING);
        runSplitTests(3, UP, DOWN, UP, LEAVING, UP);
    }

    @Test
    public void testSplitQuorumTwoDown()
    {
        runSplitTests(2, DOWN, DOWN);
        runSplitTests(2, DOWN, UP, DOWN);
        runSplitTests(3, UP, DOWN, UP, UP, DOWN);
    }

    @Test
    public void testSplitAllWithLeavingAndMovingNodes()
    {
        runSplitTests(1, DOWN);
        runSplitTests(1, UNKNOWN);
        runSplitTests(3, UP, LEAVING, DOWN);
        runSplitTests(5, UP, LEAVING, DOWN, JOINING, MOVING);
    }

    @Test
    public void testParsingAvailabilityHint()
    {
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("DOWN", "NORMAL")).isEqualTo(DOWN);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("UP", "MOVING")).isEqualTo(MOVING);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("UP", "LEAVING")).isEqualTo(LEAVING);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("UP", "NORMAL")).isEqualTo(UP);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("UP", "STARTING")).isEqualTo(UP);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("DOWN", "LEAVING")).isEqualTo(DOWN);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("DOWN", "MOVING")).isEqualTo(DOWN);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("DOWN", "NORMAL")).isEqualTo(DOWN);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("UNKNOWN", "LEAVING")).isEqualTo(UNKNOWN);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("UNKNOWN", "MOVING")).isEqualTo(UNKNOWN);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("UNKNOWN", "NORMAL")).isEqualTo(UNKNOWN);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("UP", "JOINING")).isEqualTo(JOINING);
        assertThat(PartitionedDataLayer.AvailabilityHint.fromState("randomState", "randomStatus")).isEqualTo(UNKNOWN);
    }

    @Test
    public void testAvailabilityHintComparator()
    {
        assertThat(AVAILABILITY_HINT_COMPARATOR.compare(UP, MOVING)).isEqualTo(-1);
        assertThat(AVAILABILITY_HINT_COMPARATOR.compare(LEAVING, MOVING)).isEqualTo(0);
        assertThat(AVAILABILITY_HINT_COMPARATOR.compare(UNKNOWN, MOVING)).isEqualTo(1);
        assertThat(AVAILABILITY_HINT_COMPARATOR.compare(LEAVING, UNKNOWN)).isEqualTo(-1);
        assertThat(AVAILABILITY_HINT_COMPARATOR.compare(DOWN, UNKNOWN)).isEqualTo(0);
        assertThat(AVAILABILITY_HINT_COMPARATOR.compare(JOINING, DOWN)).isEqualTo(0);
        assertThat(AVAILABILITY_HINT_COMPARATOR.compare(UP, DOWN)).isEqualTo(-1);
        assertThat(AVAILABILITY_HINT_COMPARATOR.compare(JOINING, UP)).isEqualTo(1);
    }

    @Test
    public void testSplitAll()
    {
        runSplitTests(1, DOWN);
        runSplitTests(1, UNKNOWN);
        runSplitTests(3, UP, UP, DOWN);
        runSplitTests(5, UP, UP, DOWN, UNKNOWN, UP);
    }

    @Test
    public void testValidReplicationFactor()
    {
        PartitionedDataLayer.validateReplicationFactor(ANY,
                                                       TestUtils.simpleStrategy(),
                                                       null);
        PartitionedDataLayer.validateReplicationFactor(ANY,
                                                       TestUtils.networkTopologyStrategy(),
                                                       null);
        PartitionedDataLayer.validateReplicationFactor(ANY,
                                                       TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3)),
                                                       null);
        PartitionedDataLayer.validateReplicationFactor(ANY,
                                                       TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3)),
                                                       "PV");
        PartitionedDataLayer.validateReplicationFactor(LOCAL_QUORUM,
                                                       TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3)),
                                                       "PV");
        PartitionedDataLayer.validateReplicationFactor(ALL,
                                                       TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)),
                                                       null);
        PartitionedDataLayer.validateReplicationFactor(EACH_QUORUM,
                                                       TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)),
                                                       null);
        PartitionedDataLayer.validateReplicationFactor(ANY,
                                                       TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)),
                                                       null);
    }

    @Test()
    public void testReplicationFactorDCRequired()
    {
        // DC required for DC-local consistency level
        assertThatThrownBy(() -> PartitionedDataLayer
                           .validateReplicationFactor(LOCAL_QUORUM,
                                                      TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)),
                                                      null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test()
    public void testReplicationFactorUnknownDC()
    {
        assertThatThrownBy(() -> PartitionedDataLayer
                           .validateReplicationFactor(LOCAL_QUORUM,
                                                      TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)),
                                                      "ST"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testReplicationFactorRF0()
    {
        assertThatThrownBy(() -> PartitionedDataLayer
                           .validateReplicationFactor(LOCAL_QUORUM,
                                                      TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 0)),
                                                      "MR"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testReplicationFactorEachQuorum()
    {
        assertThatThrownBy(() -> PartitionedDataLayer
                                 .validateReplicationFactor(EACH_QUORUM,
                                                            TestUtils.networkTopologyStrategy(ImmutableMap.of("PV", 3, "MR", 3)),
                                                            "MR"))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSSTableSupplier(CassandraBridge bridge)
    {
        CassandraRing ring = TestUtils.createRing(Murmur3Partitioner, 3);
        CqlTable table = TestSchema.basic(bridge).buildTable();
        DataLayer dataLayer = new JDKSerializationTests.TestPartitionedDataLayer(bridge, 4, 32, null, ring, table);
        SSTablesSupplier supplier = dataLayer.sstables(partitionId, null, new ArrayList<>());
        Set<MultipleReplicasTests.TestSSTableReader> ssTableReaders =
                supplier.openAll((ssTable, isRepairPrimary) -> new MultipleReplicasTests.TestSSTableReader(ssTable));
        assertThat(ssTableReaders).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSSTableSupplierWithMatchingFilters(CassandraBridge bridge)
    {
        CassandraRing ring = TestUtils.createRing(Partitioner.Murmur3Partitioner, 3);
        CqlTable table = TestSchema.basic(bridge).buildTable();
        DataLayer dataLayer = new JDKSerializationTests.TestPartitionedDataLayer(bridge, 4, 32, null, ring, table);

        PartitionKeyFilter filter = PartitionKeyFilter.create(ByteBuffer.wrap(RandomUtils.nextBytes(10)),
                                                              BigInteger.valueOf(-9223372036854775807L));
        SSTablesSupplier supplier = dataLayer.sstables(partitionId, null, Collections.singletonList(filter));
        Set<MultipleReplicasTests.TestSSTableReader> ssTableReaders =
                supplier.openAll((ssTable, isRepairPrimary) -> new MultipleReplicasTests.TestSSTableReader(ssTable));
        assertThat(ssTableReaders).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSSTableSupplierWithNonMatchingFilters(CassandraBridge bridge)
    {
        CassandraRing ring = TestUtils.createRing(Partitioner.Murmur3Partitioner, 3);
        CqlTable table = TestSchema.basic(bridge).buildTable();
        DataLayer dataLayer = new JDKSerializationTests.TestPartitionedDataLayer(bridge, 4, 32, null, ring, table);

        PartitionKeyFilter filter = PartitionKeyFilter.create(ByteBuffer.wrap(RandomUtils.nextBytes(10)),
                                                              BigInteger.valueOf(6917529027641081853L));
        assertThatThrownBy(() -> dataLayer.sstables(partitionId, null, Collections.singletonList(filter)))
            .isInstanceOf(NotEnoughReplicasException.class);
    }

    @Test
    public void testFiltersInRange() throws Exception
    {
        Map<Integer, Range<BigInteger>> reversePartitionMap = Collections.singletonMap(
                TaskContext.getPartitionId(), Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(2L)));
        TokenPartitioner mockPartitioner = mock(TokenPartitioner.class);
        when(mockPartitioner.reversePartitionMap()).thenReturn(reversePartitionMap);

        PartitionedDataLayer dataLayer = mock(PartitionedDataLayer.class, CALLS_REAL_METHODS);
        when(dataLayer.tokenPartitioner()).thenReturn(mockPartitioner);

        PartitionKeyFilter filterInRange = PartitionKeyFilter.create(ByteBuffer.wrap(new byte[10]),
                                                                     BigInteger.valueOf(2L));
        PartitionKeyFilter filterOutsideRange = PartitionKeyFilter.create(ByteBuffer.wrap(new byte[10]),
                                                                          BigInteger.TEN);
        PartitionKeyFilter randomFilter = mock(PartitionKeyFilter.class);
        when(randomFilter.overlaps(any())).thenReturn(true);

        assertThat(dataLayer.partitionKeyFiltersInRange(partitionId,
                                                         Collections.singletonList(randomFilter))).isNotEmpty();
        assertThat(dataLayer.partitionKeyFiltersInRange(partitionId,
                                                             Arrays.asList(filterInRange, randomFilter))).hasSize(2);
        assertThat(dataLayer.partitionKeyFiltersInRange(partitionId,
                                                             Arrays.asList(filterInRange, filterOutsideRange, randomFilter))).hasSize(2);

        // Filter does not fall in spark token range
        StreamScanner scanner = dataLayer.openCompactionScanner(partitionId,
                                                                Collections.singletonList(filterOutsideRange));
        assertThat(scanner).isInstanceOf(EmptyStreamScanner.class);
    }

    @SuppressWarnings("UnstableApiUsage")
    private static void runSplitTests(int minReplicas, PartitionedDataLayer.AvailabilityHint... availabilityHint)
    {
        int numInstances = availabilityHint.length;
        TestUtils.runTest((partitioner, dir, bridge) -> {
            CassandraRing ring = TestUtils.createRing(partitioner, numInstances);
            List<CassandraInstance> instances = new ArrayList<>(ring.instances());
            instances.sort(Comparator.comparing(CassandraInstance::nodeName));
            TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, 1, 32);
            Map<CassandraInstance, PartitionedDataLayer.AvailabilityHint> availableMap = new HashMap<>(numInstances);
            for (int instance = 0; instance < numInstances; instance++)
            {
                availableMap.put(instances.get(instance), availabilityHint[instance]);
            }

            Map<Range<BigInteger>, List<CassandraInstance>> ranges =
                    ring.getSubRanges(tokenPartitioner.getTokenRange(0)).asMapOfRanges();
            PartitionedDataLayer.ReplicaSet replicaSet =
                    PartitionedDataLayer.splitReplicas(instances, ranges, availableMap::get, minReplicas, 0);
            assertThat(replicaSet.primary()).hasSize(minReplicas);
            assertThat(replicaSet.backup()).hasSize(numInstances - minReplicas);

            List<CassandraInstance> sortedInstances = new ArrayList<>(instances);
            sortedInstances.sort(Comparator.comparing(availableMap::get, AVAILABILITY_HINT_COMPARATOR));
            for (int instance = 0; instance < sortedInstances.size(); instance++)
            {
                if (instance < minReplicas)
                {
                    assertThat(replicaSet.primary()).contains(sortedInstances.get(instance));
                }
                else
                {
                    assertThat(replicaSet.backup()).contains(sortedInstances.get(instance));
                }
            }
        });
    }

    @Test
    public void testSplitReplicas()
    {
        ReplicationFactor replicationFactor = TestUtils.networkTopologyStrategy();
        TestUtils.runTest((partitioner, dir, bridge) ->
                qt().forAll(pick(Arrays.asList(3, 32, 1024)),
                            pick(Arrays.asList(LOCAL_QUORUM, ONE, ALL, TWO)),
                            pick(Arrays.asList(1, 32, 1024)),
                            pick(Arrays.asList(1, 32, 1024)))
                    .checkAssert((numInstances, consistencyLevel, numCores, defaultParallelism) ->
                          PartitionedDataLayerTests.testSplitReplicas(TestUtils.createRing(partitioner, numInstances),
                                                                      consistencyLevel,
                                                                      defaultParallelism,
                                                                      numCores,
                                                                      replicationFactor,
                                                                      "DC1")));
    }

    @SuppressWarnings("UnstableApiUsage")
    private static void testSplitReplicas(CassandraRing ring,
                                          ConsistencyLevel consistencyLevel,
                                          int defaultParallelism,
                                          int numCores,
                                          ReplicationFactor replicationFactor,
                                          String dc)
    {
        TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, defaultParallelism, numCores);

        for (int partition = 0; partition < tokenPartitioner.numPartitions(); partition++)
        {
            Range<BigInteger> range = tokenPartitioner.getTokenRange(partition);
            Map<Range<BigInteger>, List<CassandraInstance>> subRanges = ring.getSubRanges(range).asMapOfRanges();
            Set<CassandraInstance> replicas = PartitionedDataLayer.rangesToReplicas(consistencyLevel, dc, subRanges);
            Function<CassandraInstance, PartitionedDataLayer.AvailabilityHint> availability = instances -> UP;
            int minReplicas = consistencyLevel.blockFor(replicationFactor, dc);
            PartitionedDataLayer.ReplicaSet replicaSet = PartitionedDataLayer.splitReplicas(consistencyLevel,
                                                                                            dc,
                                                                                            subRanges,
                                                                                            replicas,
                                                                                            availability,
                                                                                            minReplicas,
                                                                                            0);
            assertThat(replicaSet).isNotNull();
            assertThat(Collections.disjoint(replicaSet.primary(), replicaSet.backup())).isTrue();
            assertThat(replicaSet.primary().size() + replicaSet.backup().size()).isEqualTo(replicas.size());
        }
    }

    /**
     * Tests that the AvailabilityHint comparator correctly orders Cassandra nodes by availability priority:
     * UP nodes first, then MOVING/LEAVING nodes, and finally DOWN/UNKNOWN/JOINING nodes last.
     */
    @Test
    public void testSortingByAvailabilityHintComparator()
    {
        List<PartitionedDataLayer.AvailabilityHint> hints = Arrays.asList(UP, MOVING, LEAVING, UNKNOWN, JOINING, DOWN);

        for (int i = 0; i < 5; i++)
        {
            validateHintsSequence(hints, 1, 3);
        }

        hints = Arrays.asList(UP, UP, UP, MOVING, MOVING, LEAVING, UNKNOWN, UNKNOWN, UNKNOWN, JOINING, DOWN, DOWN, DOWN, DOWN, DOWN, DOWN);

        for (int i = 0; i < 5; i++)
        {
            validateHintsSequence(hints, 3, 6);
        }
    }

    private static void validateHintsSequence(List<PartitionedDataLayer.AvailabilityHint> hints, int index1, int index2)
    {
        List<PartitionedDataLayer.AvailabilityHint> shuffledHints = new ArrayList<>(hints);
        Collections.shuffle(shuffledHints);
        // Test expected ordering: UP > MOVING/LEAVING > UNKNOWN/JOINING/DOWN
        List<PartitionedDataLayer.AvailabilityHint> sorted = new ArrayList<>(shuffledHints);
        sorted.sort(AVAILABILITY_HINT_COMPARATOR);

        // Verify UP comes first (highest priority)
        assertThat(sorted.subList(0, index1)).contains(UP).doesNotContain(MOVING, LEAVING, UNKNOWN, JOINING, DOWN);

        // Verify MOVING, LEAVING are in the middle
        assertThat(sorted.subList(index1, index2)).contains(MOVING, LEAVING).doesNotContain(UP, DOWN, UNKNOWN, JOINING);

        // Verify DOWN, UNKNOWN, JOINING come last (lowest priority)
        assertThat(sorted.subList(index2, sorted.size())).contains(DOWN, UNKNOWN, JOINING).doesNotContain(UP, MOVING, LEAVING);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSSTablesSupplierEachQuorumConsistency(CassandraBridge bridge)
    {
        SSTablesSupplier supplier = getSsTablesSupplier(bridge, ConsistencyLevel.EACH_QUORUM);

        // Verify that the supplier is created and is of the expected type for multi-DC
        assertThat(supplier).isNotNull();
        // For EACH_QUORUM, we expect a MultiDCReplicas supplier
        assertThat(supplier).isInstanceOf(org.apache.cassandra.spark.data.partitioner.MultiDCReplicas.class);

        // Verify we can open the SSTables without errors and validate instance-specific content
        Set<MultipleReplicasTests.TestSSTableReader> ssTableReaders =
        supplier.openAll((ssTable, isRepairPrimary) -> new MultipleReplicasTests.TestSSTableReader(ssTable));
        assertThat(ssTableReaders).isNotNull();

        // For EACH_QUORUM with 2 DCs (DC1, DC2), each with 3 replicas, we should get exactly 4 readers
        // EACH_QUORUM requires 2 replicas from each DC (quorum of 3 is 2), so 2+2=4 total
        assertThat(ssTableReaders).hasSize(4);

        // Count instances from each data center
        long dc1Count = ssTableReaders.stream()
                                      .filter(reader -> reader.toString().contains("DC1-"))
                                      .count();
        long dc2Count = ssTableReaders.stream()
                                      .filter(reader -> reader.toString().contains("DC2-"))
                                      .count();

        // Validate exactly 2 instances from each DC (quorum requirement for EACH_QUORUM)
        assertThat(dc1Count).as("Should have exactly 2 instances from DC1").isEqualTo(2);
        assertThat(dc2Count).as("Should have exactly 2 instances from DC2").isEqualTo(2);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSSTablesSupplierQuorumConsistency(CassandraBridge bridge)
    {
        // Create the same multi-DC ring as EACH_QUORUM test for direct comparison
        SSTablesSupplier supplier = getSsTablesSupplier(bridge, ConsistencyLevel.QUORUM);

        // Verify that the supplier is created and is MultipleReplicas (QUORUM treats all DCs as one)
        assertThat(supplier).isNotNull();
        // For QUORUM in multi-DC, we expect a MultipleReplicas supplier
        // QUORUM considers all replicas regardless of DC, unlike EACH_QUORUM
        assertThat(supplier).isInstanceOf(org.apache.cassandra.spark.data.partitioner.MultipleReplicas.class);

        // Verify we can open the SSTables without errors and validate content
        Set<MultipleReplicasTests.TestSSTableReader> ssTableReaders =
        supplier.openAll((ssTable, isRepairPrimary) -> new MultipleReplicasTests.TestSSTableReader(ssTable));
        assertThat(ssTableReaders).isNotNull();

        // Count instances from each data center
        long dc1Count = ssTableReaders.stream()
                                      .filter(reader -> reader.toString().contains("DC1-"))
                                      .count();
        long dc2Count = ssTableReaders.stream()
                                      .filter(reader -> reader.toString().contains("DC2-"))
                                      .count();

        // For QUORUM with total RF=6 (DC1:3 + DC2:3), we need 4 replicas (quorum of 6 is (6/2)+1 = 4)
        // For QUORUM, we should have 4 total replicas, but they can be distributed across DCs
        // Unlike EACH_QUORUM which requires exactly 2 from each DC
        assertThat(dc1Count + dc2Count).as("Should have exactly 4 total instances").isEqualTo(4);

        // Verify both DCs are represented (QUORUM can pick from any DC)
        assertThat(dc1Count).as("DC1 should have at least 1 instance").isGreaterThanOrEqualTo(1);
        assertThat(dc2Count).as("DC2 should have at least 1 instance").isGreaterThanOrEqualTo(1);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSSTablesSupplierAllConsistency(CassandraBridge bridge)
    {
        // Create the same multi-DC ring as other tests for direct comparison
        SSTablesSupplier supplier = getSsTablesSupplier(bridge, ConsistencyLevel.ALL);

        // Verify that the supplier is created and is MultipleReplicas (ALL treats all DCs as one)
        assertThat(supplier).isNotNull();
        // For ALL in multi-DC, we expect a MultipleReplicas supplier
        // ALL considers all replicas regardless of DC
        assertThat(supplier).isInstanceOf(org.apache.cassandra.spark.data.partitioner.MultipleReplicas.class);

        // Verify we can open the SSTables without errors and validate content
        Set<MultipleReplicasTests.TestSSTableReader> ssTableReaders =
        supplier.openAll((ssTable, isRepairPrimary) -> new MultipleReplicasTests.TestSSTableReader(ssTable));
        assertThat(ssTableReaders).isNotNull();

        // Count instances from each data center
        long dc1Count = ssTableReaders.stream()
                                      .filter(reader -> reader.toString().contains("DC1-"))
                                      .count();
        long dc2Count = ssTableReaders.stream()
                                      .filter(reader -> reader.toString().contains("DC2-"))
                                      .count();

        // For ALL with total RF=6 (DC1:3 + DC2:3), we need all 6 replicas
        // ALL requires responses from every replica
        assertThat(dc1Count + dc2Count).as("Should have exactly 6 total instances (all replicas)").isEqualTo(6);

        // Verify both DCs are fully represented (ALL needs all replicas)
        assertThat(dc1Count).as("DC1 should have exactly 3 instances").isEqualTo(3);
        assertThat(dc2Count).as("DC2 should have exactly 3 instances").isEqualTo(3);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.spark.data.VersionRunner#bridges")
    public void testSSTablesSupplierAnyConsistency(CassandraBridge bridge)
    {
        // Create the same multi-DC ring as other tests for direct comparison
        SSTablesSupplier supplier = getSsTablesSupplier(bridge, ConsistencyLevel.ANY);

        // Verify that the supplier is created and is MultipleReplicas (ANY treats all DCs as one)
        assertThat(supplier).isNotNull();
        // For ANY in multi-DC, we expect a MultipleReplicas supplier
        // ANY only requires one replica to acknowledge, but we still need to read from multiple for consistency
        assertThat(supplier).isInstanceOf(org.apache.cassandra.spark.data.partitioner.MultipleReplicas.class);

        // Verify we can open the SSTables without errors and validate content
        Set<MultipleReplicasTests.TestSSTableReader> ssTableReaders =
        supplier.openAll((ssTable, isRepairPrimary) -> new MultipleReplicasTests.TestSSTableReader(ssTable));
        assertThat(ssTableReaders).isNotNull();

        // For ANY with total RF=6 (DC1:3 + DC2:3), ANY requires only 1 replica but for reads
        // we need to ensure data consistency, so we expect exact 1 replica
        assertThat(ssTableReaders.size()).as("Should have exactly 1 total instance").isEqualTo(1);
    }

    private SSTablesSupplier getSsTablesSupplier(CassandraBridge bridge, ConsistencyLevel consistencyLevel)
    {
        // Create a multi-DC ring
        Map<String, Integer> datacenters = Map.of("DC1", 3, "DC2", 3);
        CassandraRing ring = TestUtils.createRing(Partitioner.Murmur3Partitioner, datacenters);
        CqlTable table = TestSchema.basic(bridge).buildTable();

        // Create a PartitionedDataLayer
        TestPartitionedDataLayerWithConsistencyLevel dataLayer = new TestPartitionedDataLayerWithConsistencyLevel(
        bridge, 4, 32, null, ring, table, consistencyLevel);

        return dataLayer.sstables(partitionId, null, new ArrayList<>());
    }

    /**
     * Test implementation of PartitionedDataLayer that accepts any consistency levelorg.apache.cassandra.distributed.impl
     */
    private static class TestPartitionedDataLayerWithConsistencyLevel extends PartitionedDataLayer
    {
        private final CassandraBridge bridge;
        private final CassandraRing ring;
        private final CqlTable cqlTable;
        private final TokenPartitioner tokenPartitioner;
        private final String jobId;

        TestPartitionedDataLayerWithConsistencyLevel(CassandraBridge bridge,
                                                     int defaultParallelism,
                                                     int numCores,
                                                     String dc,
                                                     CassandraRing ring,
                                                     CqlTable cqlTable,
                                                     ConsistencyLevel consistencyLevel)
        {
            super(consistencyLevel, dc);
            this.bridge = bridge;
            this.ring = ring;
            this.cqlTable = cqlTable;
            this.tokenPartitioner = new TokenPartitioner(ring, defaultParallelism, numCores);
            this.jobId = UUID.randomUUID().toString();
        }

        public CompletableFuture<Stream<SSTable>> listInstance(int partitionId,
                                                               @NotNull Range<BigInteger> range,
                                                               @NotNull CassandraInstance instance)
        {
            // Return one instance-specific SSTable
            String dc = instance.dataCenter();
            String instanceName = instance.nodeName();
            List<SSTable> testSSTables = List.of(
            new TestSSTable(dc + "-" + instanceName + "-Data.db", dc)
            );
            return CompletableFuture.completedFuture(testSSTables.stream());
        }

        @Override
        public CassandraBridge bridge()
        {
            return bridge;
        }

        @Override
        public CassandraRing ring()
        {
            return ring;
        }

        @Override
        public String jobId()
        {
            return jobId;
        }

        @Override
        public TokenPartitioner tokenPartitioner()
        {
            return tokenPartitioner;
        }

        public ReplicationFactor replicationFactor(String keyspace)
        {
            return new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, Map.of("DC1", 3, "DC2", 3));
        }

        @Override
        public CqlTable cqlTable()
        {
            return cqlTable;
        }

        public TimeProvider timeProvider()
        {
            return null;
        }

        protected ExecutorService executorService()
        {
            return java.util.concurrent.Executors.newSingleThreadExecutor();
        }
    }

    /**
     * Simple test implementation of SSTable for testing purposes
     */
    private static class TestSSTable extends SSTable
    {
        private final String filename;
        private final String dataCenter;

        TestSSTable(String filename, String dataCenter)
        {
            this.filename = filename;
            this.dataCenter = dataCenter;
        }

        @Override
        protected InputStream openInputStream(FileType fileType)
        {
            return null;
        }

        @Override
        public long length(FileType fileType)
        {
            return 1024; // Mock file size
        }

        @Override
        public boolean isMissing(FileType fileType)
        {
            return false;
        }

        @Override
        public String getDataFileName()
        {
            return filename;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            TestSSTable that = (TestSSTable) o;
            return Objects.equals(filename, that.filename) && Objects.equals(dataCenter, that.dataCenter);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(filename, dataCenter);
        }

        @Override
        public String toString()
        {
            return "TestSSTable{filename='" + filename + "', dataCenter='" + dataCenter + "'}";
        }
    }
}
