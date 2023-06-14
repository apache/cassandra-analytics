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

package org.apache.cassandra.spark.bulkwriter;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TokenPartitionerTest
{
    private TokenPartitioner partitioner;

    @BeforeEach
    public void createConfig()
    {
    }

    @Test
    public void testOneSplit()
    {
        CassandraRing<RingInstance> ring = RingUtils.buildRing(0, "app", "cluster", "DC1", "test");
        partitioner = new TokenPartitioner(ring, 1, 2, 1, false);
        assertEquals(4, partitioner.numPartitions());
        assertEquals(0, getPartitionForToken(new BigInteger("-9223372036854775808")));
        assertEquals(0, getPartitionForToken(0));
        assertEquals(1, getPartitionForToken(1));
        assertEquals(2, getPartitionForToken(100_001));
        assertEquals(3, getPartitionForToken(200_001));
        assertEquals(3, getPartitionForToken(new BigInteger("9223372036854775807")));
    }

    @Test
    public void testTwoSplits()
    {
        CassandraRing<RingInstance> ring = RingUtils.buildRing(0, "app", "cluster", "DC1", "test");
        partitioner = new TokenPartitioner(ring, 2, 2, 1, false);
        assertEquals(10, partitioner.numPartitions());
        assertEquals(0, getPartitionForToken(new BigInteger("-4611686018427387905")));
        assertEquals(1, getPartitionForToken(new BigInteger("-4611686018427387904")));
        assertEquals(1, getPartitionForToken(-1));
        assertEquals(2, getPartitionForToken(0));  // Single token range
        assertEquals(3, getPartitionForToken(1));
        assertEquals(3, getPartitionForToken(50));
        assertEquals(4, getPartitionForToken(51000));
        assertEquals(4, getPartitionForToken(51100));
        assertEquals(5, getPartitionForToken(100001));
        assertEquals(5, getPartitionForToken(100150));
        assertEquals(5, getPartitionForToken(150000));
        assertEquals(6, getPartitionForToken(150001));
        assertEquals(6, getPartitionForToken(200000));
        assertEquals(7, getPartitionForToken(200001));
        assertEquals(7, getPartitionForToken(new BigInteger("4611686018427388003")));
        assertEquals(7, getPartitionForToken(new BigInteger("4611686018427487903")));
        assertEquals(8, getPartitionForToken(new BigInteger("4611686018427487904")));
        assertEquals(9, getPartitionForToken(new BigInteger("9223372036854775807")));  // Single token range
    }

    // It is possible for a keyspace to replicate to fewer than all datacenters. In these cases, the
    // check for partitions > instances is incorrect, because it was using the total number of instances
    // in the cluster (ring.instances), not the number of instances included in the RF of the keyspace.
    // Instead, we check ring.getTokenRanges().keySet().size(), which returns the list of unique instances
    // actually participating in the replication of data for this keyspace.
    // Without the fix, this test would throw during validation.
    @Test
    public void testReplicationFactorInOneDCOnly()
    {
        CassandraRing<RingInstance> ring = RingUtils.buildRing(0, "app", "cluster", ImmutableMap.of("DC1", 3, "DC2", 0), "test", 3);
        partitioner = new TokenPartitioner(ring, 1, 2, 1, false);
        assertEquals(4, partitioner.numPartitions());
        assertEquals(0, getPartitionForToken(new BigInteger("-9223372036854775808")));
        assertEquals(0, getPartitionForToken(0));
        assertEquals(1, getPartitionForToken(100000));
        assertEquals(2, getPartitionForToken(100001));
        assertEquals(3, getPartitionForToken(200001));
        assertEquals(3, getPartitionForToken(new BigInteger("9223372036854775807")));
    }

    @Test
    public void testSplitCalculationsUsingCores()
    {
        CassandraRing<RingInstance> ring = RingUtils.buildRing(0, "app", "cluster", "DC1", "test");
        // When passed "-1" for numberSplits, the token partitioner should calculate it on its own based on
        // the number of cores. This ring has 4 ranges when no splits are used, therefore we expect the number
        // of splits to be 25 for 100 cores and a default parallelism of 50 (as we take the max of the two).
        // This results in slightly over 100 partitions, which is what we're looking for.
        partitioner = new TokenPartitioner(ring, -1, 50, 100, false);
        assertEquals(25, partitioner.numSplits());
        assertThat(partitioner.numPartitions(), greaterThanOrEqualTo(100));
    }

    @Test
    public void testSplitCalculationsUsingDefaultParallelism()
    {
        CassandraRing<RingInstance> ring = RingUtils.buildRing(0, "app", "cluster", "DC1", "test");
        // When passed "-1" for numberSplits, the token partitioner should calculate it on its own based on
        // the number of cores. This ring has 4 ranges when no splits are used, therefore we expect the number
        // of splits to be 50 for 100 cores and a default parallelism of 200 (as we take the max of the two).
        // This results in slightly over 200 partitions, which is what we're looking for.
        partitioner = new TokenPartitioner(ring, -1, 200, 100, false);
        assertEquals(50, partitioner.numSplits());
        assertThat(partitioner.numPartitions(), greaterThanOrEqualTo(200));
    }

    @Test
    public void testSplitCalculationWithMultipleDcs()
    {
        ImmutableMap<String, Integer> dcMap = ImmutableMap.<String, Integer>builder()
                                                                .put("DC1", 3)
                                                                .put("DC2", 3)
                                                                .put("DC3", 3)
                                                                .put("DC4", 3)
                                                                .build();
        CassandraRing<RingInstance> ring = RingUtils.buildRing(0, "app", "cluster", dcMap, "test", 20);
        assertEquals(80, ring.getInstances().size());
        partitioner = new TokenPartitioner(ring, -1, 1, 750, false);
        assertEquals(10, partitioner.numSplits());
        assertThat(partitioner.numPartitions(), greaterThanOrEqualTo(200));
    }

    private int getPartitionForToken(int token)
    {
        return getPartitionForToken(BigInteger.valueOf(token));
    }

    private int getPartitionForToken(BigInteger token)
    {
        return partitioner.getPartition(new DecoratedKey(token, ByteBuffer.allocate(0)));
    }
}
