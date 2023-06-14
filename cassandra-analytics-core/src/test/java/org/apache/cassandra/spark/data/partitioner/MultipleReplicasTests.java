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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.PartitionedDataLayer;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.stats.Stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.quicktheories.QuickTheory.qt;

public class MultipleReplicasTests
{
    private static final int[] NUM_SSTABLES = new int[]{3, 5, 7, 11, 13, 17, 19, 23};

    @Test
    public void testRF1AllUp()
    {
        runTest(3, 1, 0, 0);
    }

    @Test
    public void testRF1BackupsDown()
    {
        runTest(3, 1, 0, 2);
    }

    @Test
    public void testRF1SomeDown()
    {
        runTest(3, 1, 1, 1);
    }

    @Test
    public void testRF3QuorumAllUp()
    {
        runTest(3, 2, 0, 0);
    }

    @Test
    public void testRF3QuorumBackupInstanceDown()
    {
        runTest(3, 2, 0, 1);
    }

    @Test
    public void testRF3QuorumPrimaryInstanceDown()
    {
        runTest(3, 2, 1, 0);
    }

    @Test
    public void testRF5QuorumTwoPrimaryInstanceDown()
    {
        runTest(5, 3, 2, 0);
    }

    @Test()
    public void testRF1NotEnoughReplicas()
    {
        assertThrows(AssertionError.class,
                     () -> runTest(1, 1, 1, 0)
        );
    }

    @Test()
    public void testRF3QuorumNotEnoughReplicas()
    {
        assertThrows(AssertionError.class,
                     () -> runTest(3, 2, 1, 1)
        );
    }

    @Test()
    public void testRFAllNotEnoughReplicas()
    {
        assertThrows(AssertionError.class,
                     () ->
                     runTest(3, 3, 1, 0)
        );
    }

    private static void runTest(int numInstances, int rfFactor, int numDownPrimaryInstances, int numDownBackupInstances)
    {
        qt().forAll(TestUtils.partitioners()).checkAssert(partitioner -> {
            // Mock CassandraRing/Instances and DataLayer
            CassandraRing ring = TestUtils.createRing(partitioner, numInstances);
            List<CassandraInstance> instances = new ArrayList<>(ring.instances());
            PartitionedDataLayer dataLayer = mock(PartitionedDataLayer.class);
            Range<BigInteger> range = Range.closed(partitioner.minToken(), partitioner.maxToken());
            Set<SingleReplica> primaryReplicas = new HashSet<>(rfFactor);
            Set<SingleReplica> backupReplicas = new HashSet<>(numInstances - rfFactor);
            int expectedSSTables = 0;
            int upInstances = 0;
            List<CassandraInstance> requestedInstances = new ArrayList<>();

            // Mock some primary and backup replicas with a different number of SSTables and some UP some DOWN
            for (int position = 0; position < rfFactor; position++)
            {
                boolean isDown = position < numDownPrimaryInstances;
                int numSSTables = NUM_SSTABLES[position];
                requestedInstances.add(instances.get(position));
                if (!isDown)
                {
                    upInstances++;
                    expectedSSTables += numSSTables;
                }
                primaryReplicas.add(mockReplica(instances.get(position), dataLayer, range, numSSTables, isDown));
            }
            for (int position = rfFactor; position < numInstances; position++)
            {
                boolean isDown = (position - rfFactor) < numDownBackupInstances;
                int numSSTables = NUM_SSTABLES[position];
                SingleReplica replica = mockReplica(instances.get(position), dataLayer, range, numSSTables, isDown);
                if (!isDown && upInstances < rfFactor)
                {
                    upInstances++;
                    expectedSSTables += numSSTables;
                    requestedInstances.add(instances.get(position));
                }
                backupReplicas.add(replica);
            }

            // Open replicas and verify correct number of SSTables opened should only throw NotEnoughReplicasException
            // if insufficient primary or backup replicas available to meet consistency level
            MultipleReplicas replicas = new MultipleReplicas(primaryReplicas, backupReplicas, Stats.DoNothingStats.INSTANCE);
            Set<TestSSTableReader> readers = replicas.openAll((ssTable, isRepairPrimary) -> new TestSSTableReader(ssTable));
            assertEquals(expectedSSTables, readers.size());

            // Verify list instance attempted on all primary instances
            // and any backup instances that needed to be called to meet consistency
            for (CassandraInstance instance : requestedInstances)
            {
                verify(dataLayer, times(1)).listInstance(eq(0), eq(range), eq(instance));
            }
        });
    }

    private static SingleReplica mockReplica(CassandraInstance instance,
                                             PartitionedDataLayer dataLayer,
                                             Range<BigInteger> range,
                                             int numSSTables,
                                             boolean shouldFail)
    {
        when(dataLayer.listInstance(eq(0), eq(range), eq(instance))).thenAnswer(invocation -> {
            if (shouldFail)
            {
                CompletableFuture<Stream<SSTable>> exceptionally = new CompletableFuture<>();
                exceptionally.completeExceptionally(new RuntimeException("Something went wrong"));
                return exceptionally;
            }
            return CompletableFuture.completedFuture(IntStream.range(0, numSSTables)
                                                              .mapToObj(ssTable -> SingleReplicaTests.mockSSTable()));
        });
        return new SingleReplica(instance, dataLayer, range, 0, SingleReplicaTests.EXECUTOR, true);
    }

    public static class TestSSTableReader implements SparkSSTableReader
    {
        public TestSSTableReader(SSTable ssTable)
        {
        }

        public BigInteger firstToken()
        {
            return BigInteger.valueOf(-4099276460824344804L);
        }

        public BigInteger lastToken()
        {
            return BigInteger.valueOf(2049638230412172401L);
        }

        public boolean ignore()
        {
            return false;
        }
    }
}
