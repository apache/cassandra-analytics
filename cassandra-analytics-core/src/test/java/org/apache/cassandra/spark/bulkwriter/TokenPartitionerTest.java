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

import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;

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
        TokenRangeMapping<RingInstance> tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), 3);
        partitioner = new TokenPartitioner(tokenRangeMapping, 1, 2, 1, false);
        assertEquals(4, partitioner.numPartitions());
        assertEquals(0, partitionForToken(new BigInteger("-9223372036854775807")));
        assertEquals(0, partitionForToken(0));
        assertEquals(1, partitionForToken(1));
        assertEquals(2, partitionForToken(100_001));
        assertEquals(3, partitionForToken(200_001));
        assertEquals(3, partitionForToken(new BigInteger("9223372036854775807")));
    }

    @Test
    public void testTwoSplits()
    {
        // There are 4 unwrapped ranges; each range is further split into 2 sub-ranges
        TokenRangeMapping<RingInstance> tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), 3);
        partitioner = new TokenPartitioner(tokenRangeMapping, 2, 2, 1, false);
        // result into the following ranges:
        // (-9223372036854775808‥-4611686018427387904]=0,
        // (-4611686018427387904‥0]=1,
        // (0‥50000]=2,
        // (50000‥100000]=3,
        // (100000‥150000]=4,
        // (150000‥200000]=5,
        // (200000‥4611686018427487904]=6,
        // (4611686018427487904‥9223372036854775807]=7
        assertEquals(8, partitioner.numPartitions());

        // Partition 0 -
        // Test with the min token of Murmur3Partitioner. It should not exit.
        // However, spark partitioner does not permit negative values, so it assigns the token to partition 0 artificially
        assertEquals(0, partitionForToken(new BigInteger("-9223372036854775808")));
        assertEquals(0, partitionForToken(new BigInteger("-9223372036854775807")));
        // Inclusive Boundary: -4611686018427387904
        assertEquals(0, partitionForToken(new BigInteger("-4611686018427387904")));

        // Partition 1 - Exclusive Boundary: -4611686018427387904
        assertEquals(1, partitionForToken(new BigInteger("-4611686018427387903")));
        // Inclusive Boundary: 0
        assertEquals(1, partitionForToken(0));

        // Partition 2 -
        assertEquals(2, partitionForToken(1));
        assertEquals(2, partitionForToken(50));

        // Partition 3 -
        assertEquals(3, partitionForToken(51000));
        assertEquals(3, partitionForToken(51100));

        // Partition 4 -
        assertEquals(4, partitionForToken(100001));
        assertEquals(4, partitionForToken(100150));
        assertEquals(4, partitionForToken(150000));

        // Partition 5 -
        assertEquals(5, partitionForToken(150001));
        assertEquals(5, partitionForToken(200000));

        // Partition 6 -
        assertEquals(6, partitionForToken(200001));
        assertEquals(6, partitionForToken(new BigInteger("4611686018427388003")));
        assertEquals(6, partitionForToken(new BigInteger("4611686018427487904")));

        // Partition 7 - Exclusive Boundary: 4611686018427487904
        assertEquals(7, partitionForToken(new BigInteger("4611686018427487905"))); // boundary
        // Inclusive Boundary: 9223372036854775807
        assertEquals(7, partitionForToken(new BigInteger("9223372036854775807")));
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
        TokenRangeMapping<RingInstance> tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3, "DC2", 0), 3);
        partitioner = new TokenPartitioner(tokenRangeMapping, 1, 2, 1, false);
        assertEquals(4, partitioner.numPartitions());
        assertEquals(0, partitionForToken(new BigInteger("-9223372036854775807")));
        assertEquals(0, partitionForToken(0));
        assertEquals(1, partitionForToken(100000));
        assertEquals(2, partitionForToken(100001));
        assertEquals(3, partitionForToken(200001));
        assertEquals(3, partitionForToken(new BigInteger("9223372036854775807")));
    }

    @Test
    public void testSplitCalculationsUsingCores()
    {
        TokenRangeMapping<RingInstance> tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), 3);
        // When passed "-1" for numberSplits, the token partitioner should calculate it on its own based on the number of cores
        // This ring has 4 ranges when no splits are used, therefore we expect the number of splits to be 25 for 100 cores
        // and a default parallelism of 50 (as we take the max of the two)
        // This results in slightly over 100 partitions, which is what we're looking for
        partitioner = new TokenPartitioner(tokenRangeMapping, -1, 50, 100, false);
        assertEquals(25, partitioner.numSplits());
        assertThat(partitioner.numPartitions(), greaterThanOrEqualTo(100));
    }

    @Test
    public void testSplitCalculationsUsingDefaultParallelism()
    {
        TokenRangeMapping<RingInstance> tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), 3);
        // When passed "-1" for numberSplits, the token partitioner should calculate it on its own based on the number of cores
        // This ring has 4 ranges when no splits are used, therefore we expect the number of splits to be 50 for 100 cores
        // and a default parallelism of 200 (as we take the max of the two)
        // This results in slightly over 200 partitions, which is what we're looking for
        partitioner = new TokenPartitioner(tokenRangeMapping, -1, 200, 100, false);
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
        TokenRangeMapping<RingInstance> tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, dcMap, 20);
        partitioner = new TokenPartitioner(tokenRangeMapping, -1, 1, 750, false);
        assertEquals(10, partitioner.numSplits());
        assertThat(partitioner.numPartitions(), greaterThanOrEqualTo(200));
    }

    private int partitionForToken(int token)
    {
        return partitionForToken(BigInteger.valueOf(token));
    }

    private int partitionForToken(BigInteger token)
    {
        return partitioner.getPartition(new DecoratedKey(token, ByteBuffer.allocate(0)));
    }
}
