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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

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
import org.apache.cassandra.spark.sparksql.filters.SSTableTimeRangeFilter;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.TaskContext;

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
                                                                Collections.singletonList(filterOutsideRange),
                                                                SSTableTimeRangeFilter.ALL);
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

    private static void validateHintsSequence(List<PartitionedDataLayer.AvailabilityHint> hints, int upCount, int movingOrLeavingCount)
    {
        List<PartitionedDataLayer.AvailabilityHint> shuffledHints = new ArrayList<>(hints);
        Collections.shuffle(shuffledHints);
        // Test expected ordering: UP > MOVING/LEAVING > UNKNOWN/JOINING/DOWN
        List<PartitionedDataLayer.AvailabilityHint> sorted = new ArrayList<>(shuffledHints);
        sorted.sort(AVAILABILITY_HINT_COMPARATOR);

        // Verify UP comes first (highest priority)
        assertThat(sorted.subList(0, upCount)).contains(UP).doesNotContain(MOVING, LEAVING, UNKNOWN, JOINING, DOWN);

        // Verify MOVING, LEAVING are in the middle
        assertThat(sorted.subList(upCount, movingOrLeavingCount)).contains(MOVING, LEAVING).doesNotContain(UP, DOWN, UNKNOWN, JOINING);

        // Verify DOWN, UNKNOWN, JOINING come last (lowest priority)
        assertThat(sorted.subList(movingOrLeavingCount, sorted.size())).contains(DOWN, UNKNOWN, JOINING).doesNotContain(UP, MOVING, LEAVING);
    }

    /**
     * Tied availability + primary-hint=true on one instance: that instance is biased into the
     * primary set ahead of its peers (whose hint returns false). Confirms the secondary
     * comparator activates only when the availability bucket is shared.
     */
    @Test
    public void testSplitReplicasPrimaryHintWinsOnAvailabilityTie()
    {
        CassandraInstance a = new CassandraInstance("100", "node-a", "DC1");
        CassandraInstance b = new CassandraInstance("200", "node-b", "DC1");
        CassandraInstance c = new CassandraInstance("300", "node-c", "DC1");
        List<CassandraInstance> all = Arrays.asList(a, b, c);

        Map<Range<BigInteger>, List<CassandraInstance>> ranges = Collections.singletonMap(
            Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(300L)), all);
        Function<CassandraInstance, PartitionedDataLayer.AvailabilityHint> uniformlyUp = instance -> UP;
        Predicate<CassandraInstance> hint = instance -> instance.equals(b);

        PartitionedDataLayer.ReplicaSet replicaSet =
            PartitionedDataLayer.splitReplicas(all, ranges, uniformlyUp, hint, 1, 0);

        assertThat(replicaSet.primary())
            .as("hint=true should pull node-b into the primary set when availability ties")
            .containsExactly(b);
        assertThat(replicaSet.backup()).containsExactlyInAnyOrder(a, c);
    }

    /**
     * Availability dominates the hint: a hint=true instance with worse availability stays in
     * backup; a hint=false instance with better availability stays in primary.
     */
    @Test
    public void testSplitReplicasAvailabilityDominatesPrimaryHint()
    {
        CassandraInstance healthyButNotHinted = new CassandraInstance("100", "node-a", "DC1");
        CassandraInstance downButHinted = new CassandraInstance("200", "node-b", "DC1");
        List<CassandraInstance> all = Arrays.asList(healthyButNotHinted, downButHinted);

        Map<Range<BigInteger>, List<CassandraInstance>> ranges = Collections.singletonMap(
            Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(200L)), all);
        Map<CassandraInstance, PartitionedDataLayer.AvailabilityHint> availability = new HashMap<>();
        availability.put(healthyButNotHinted, UP);
        availability.put(downButHinted, DOWN);

        Predicate<CassandraInstance> hint = instance -> instance.equals(downButHinted);

        PartitionedDataLayer.ReplicaSet replicaSet =
            PartitionedDataLayer.splitReplicas(all, ranges, availability::get, hint, 1, 0);

        assertThat(replicaSet.primary())
            .as("UP non-hinted instance must outrank DOWN hinted instance")
            .containsExactly(healthyButNotHinted);
        assertThat(replicaSet.backup()).containsExactly(downButHinted);
    }

    /**
     * The pre-existing 5-arg {@code splitReplicas} delegates to the new 6-arg overload with a
     * no-op hint predicate. This regression test pins that callers without a hint see ordering
     * identical to the comparator-only path.
     */
    @Test
    public void testSplitReplicasNoHintPreservesAvailabilityOrdering()
    {
        CassandraInstance up = new CassandraInstance("100", "node-up", "DC1");
        CassandraInstance leaving = new CassandraInstance("200", "node-leaving", "DC1");
        CassandraInstance down = new CassandraInstance("300", "node-down", "DC1");
        List<CassandraInstance> all = Arrays.asList(up, leaving, down);

        Map<Range<BigInteger>, List<CassandraInstance>> ranges = Collections.singletonMap(
            Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(300L)), all);
        Map<CassandraInstance, PartitionedDataLayer.AvailabilityHint> availability = new HashMap<>();
        availability.put(up, UP);
        availability.put(leaving, LEAVING);
        availability.put(down, DOWN);

        PartitionedDataLayer.ReplicaSet replicaSet =
            PartitionedDataLayer.splitReplicas(all, ranges, availability::get, 1, 0);

        assertThat(replicaSet.primary())
            .as("availability ordering: UP must be the only primary")
            .containsExactly(up);
        assertThat(replicaSet.backup())
            .as("LEAVING and DOWN remain in the backup set, with LEAVING ahead of DOWN")
            .containsExactlyInAnyOrder(leaving, down);
    }
}
