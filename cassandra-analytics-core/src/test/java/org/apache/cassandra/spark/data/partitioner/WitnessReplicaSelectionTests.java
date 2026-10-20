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

package org.apache.cassandra.spark.data.partitioner;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.SSTablesSupplier;
import org.jetbrains.annotations.NotNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Replica selection for keyspaces with witness replicas, which do not hold complete data and so must never be read
 */
public class WitnessReplicaSelectionTests
{
    private static final CassandraInstance N1 = new CassandraInstance("0", "n1", "dc1");
    private static final CassandraInstance N2 = new CassandraInstance("100", "n2", "dc1");
    private static final CassandraInstance N3 = new CassandraInstance("200", "n3", "dc1");

    private static final Range<BigInteger> R1 = Range.openClosed(Partitioner.Murmur3Partitioner.minToken(), BigInteger.ZERO);
    private static final Range<BigInteger> R2 = Range.openClosed(BigInteger.ZERO, BigInteger.valueOf(100L));
    private static final Range<BigInteger> R3 = Range.openClosed(BigInteger.valueOf(100L), BigInteger.valueOf(200L));
    private static final Range<BigInteger> R4 = Range.openClosed(BigInteger.valueOf(200L), Partitioner.Murmur3Partitioner.maxToken());

    /**
     * Three nodes with {@code '3/1'}: every node replicates every range and is the witness for some of them
     */
    private static CassandraRing witnessRing()
    {
        Map<Range<BigInteger>, List<CassandraInstance>> replicas = new LinkedHashMap<>();
        Map<Range<BigInteger>, List<CassandraInstance>> witnesses = new LinkedHashMap<>();
        for (Range<BigInteger> range : Arrays.asList(R1, R2, R3, R4))
        {
            replicas.put(range, Arrays.asList(N1, N2, N3));
        }
        witnesses.put(R1, Collections.singletonList(N3));
        witnesses.put(R2, Collections.singletonList(N1));
        witnesses.put(R3, Collections.singletonList(N2));
        witnesses.put(R4, Collections.singletonList(N3));
        return new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                 new ReplicationFactor(ImmutableMap.of(
                                 "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                 "dc1", "3/1")),
                                 Arrays.asList(N1, N2, N3), replicas, witnesses);
    }

    /**
     * Records which instances each Spark partition reads from
     */
    private static class RecordingDataLayer extends JDKSerializationTests.TestPartitionedDataLayer
    {
        private final Map<Integer, Set<String>> readFrom = new ConcurrentHashMap<>();

        RecordingDataLayer(CassandraRing ring, ConsistencyLevel consistencyLevel)
        {
            super(null, 4, 32, "dc1", ring, null);
            this.consistencyLevel = consistencyLevel;
        }

        @Override
        public CompletableFuture<Stream<SSTable>> listInstance(int partitionId,
                                                               @NotNull Range<BigInteger> range,
                                                               @NotNull CassandraInstance instance)
        {
            readFrom.computeIfAbsent(partitionId, id -> ConcurrentHashMap.newKeySet()).add(instance.nodeName());
            return CompletableFuture.completedFuture(Stream.of());
        }
    }

    private static Set<String> expectedFullReplicas(Range<BigInteger> partitionRange)
    {
        // Partitions never span Cassandra ranges, so the upper bound identifies the range
        BigInteger upper = partitionRange.upperEndpoint();
        Set<String> all = new TreeSet<>(Arrays.asList("n1", "n2", "n3"));
        if (R1.contains(upper) || R4.contains(upper))
        {
            all.remove("n3");
        }
        else if (R2.contains(upper))
        {
            all.remove("n1");
        }
        else
        {
            all.remove("n2");
        }
        return all;
    }

    private static void assertReadsOnlyFullReplicas(RecordingDataLayer dataLayer)
    {
        TokenPartitioner partitioner = dataLayer.tokenPartitioner();
        for (int partitionId = 0; partitionId < partitioner.numPartitions(); partitionId++)
        {
            SSTablesSupplier supplier = dataLayer.sstables(partitionId, null, Collections.emptyList());
            supplier.openAll((ssTable, isRepairPrimary) -> new MultipleReplicasTests.TestSSTableReader(ssTable));

            Range<BigInteger> partitionRange = partitioner.getTokenRange(partitionId);
            assertThat(new TreeSet<>(dataLayer.readFrom.get(partitionId)))
            .as("partition %d over %s must read only from the full replicas", partitionId, partitionRange)
            .isEqualTo(expectedFullReplicas(partitionRange));
        }
    }

    @Test
    public void testWitnessIsNeverReadFrom()
    {
        assertReadsOnlyFullReplicas(new RecordingDataLayer(witnessRing(), ConsistencyLevel.LOCAL_QUORUM));
    }

    @Test
    public void testAllReadsEveryFullReplica()
    {
        // ALL counts full replicas only, as witnesses are never read; otherwise it could never be satisfied
        assertReadsOnlyFullReplicas(new RecordingDataLayer(witnessRing(), ConsistencyLevel.ALL));
    }

    @Test
    public void testNotEnoughFullReplicasNamesTheExcludedWitness()
    {
        // A range reported with one full replica and its witness, as when a datacenter is short of nodes
        Map<Range<BigInteger>, List<CassandraInstance>> replicas = new LinkedHashMap<>();
        replicas.put(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(), Partitioner.Murmur3Partitioner.maxToken()),
                     Arrays.asList(N1, N3));
        Map<Range<BigInteger>, List<CassandraInstance>> witnesses = new LinkedHashMap<>();
        witnesses.put(Range.openClosed(Partitioner.Murmur3Partitioner.minToken(), Partitioner.Murmur3Partitioner.maxToken()),
                      Collections.singletonList(N3));
        CassandraRing ring = new CassandraRing(Partitioner.Murmur3Partitioner, "test",
                                               new ReplicationFactor(ImmutableMap.of(
                                               "class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                               "dc1", "3/1")),
                                               Arrays.asList(N1, N2, N3), replicas, witnesses);
        RecordingDataLayer dataLayer = new RecordingDataLayer(ring, ConsistencyLevel.LOCAL_QUORUM);

        assertThatThrownBy(() -> dataLayer.sstables(0, null, Collections.emptyList()))
        .isInstanceOf(NotEnoughReplicasException.class)
        .hasMessageContaining("required 2 but only 1 found")
        .hasMessageContaining("witness replicas are excluded")
        .hasMessageContaining("[n3]");
    }
}
