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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.utils.RangeUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        assertThat(partitioner.numPartitions()).isEqualTo(4);
        assertThat(partitionForToken(new BigInteger("-9223372036854775807"))).isEqualTo(0);
        assertThat(partitionForToken(0)).isEqualTo(0);
        assertThat(partitionForToken(1)).isEqualTo(1);
        assertThat(partitionForToken(100_001)).isEqualTo(2);
        assertThat(partitionForToken(200_001)).isEqualTo(3);
        assertThat(partitionForToken(new BigInteger("9223372036854775807"))).isEqualTo(3);
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
        assertThat(partitioner.numPartitions()).isEqualTo(8);

        // Partition 0 -
        // Test with the min token of Murmur3Partitioner. It should not exit.
        // However, spark partitioner does not permit negative values, so it assigns the token to partition 0 artificially
        assertThat(partitionForToken(new BigInteger("-9223372036854775808"))).isEqualTo(0);
        assertThat(partitionForToken(new BigInteger("-9223372036854775807"))).isEqualTo(0);
        // Inclusive Boundary: -4611686018427387904
        assertThat(partitionForToken(new BigInteger("-4611686018427387904"))).isEqualTo(0);

        // Partition 1 - Exclusive Boundary: -4611686018427387904
        assertThat(partitionForToken(new BigInteger("-4611686018427387903"))).isEqualTo(1);
        // Inclusive Boundary: 0
        assertThat(partitionForToken(0)).isEqualTo(1);

        // Partition 2 -
        assertThat(partitionForToken(1)).isEqualTo(2);
        assertThat(partitionForToken(50)).isEqualTo(2);

        // Partition 3 -
        assertThat(partitionForToken(51000)).isEqualTo(3);
        assertThat(partitionForToken(51100)).isEqualTo(3);

        // Partition 4 -
        assertThat(partitionForToken(100001)).isEqualTo(4);
        assertThat(partitionForToken(100150)).isEqualTo(4);
        assertThat(partitionForToken(150000)).isEqualTo(4);

        // Partition 5 -
        assertThat(partitionForToken(150001)).isEqualTo(5);
        assertThat(partitionForToken(200000)).isEqualTo(5);

        // Partition 6 -
        assertThat(partitionForToken(200001)).isEqualTo(6);
        assertThat(partitionForToken(new BigInteger("4611686018427388003"))).isEqualTo(6);
        assertThat(partitionForToken(new BigInteger("4611686018427487904"))).isEqualTo(6);

        // Partition 7 - Exclusive Boundary: 4611686018427487904
        assertThat(partitionForToken(new BigInteger("4611686018427487905"))).isEqualTo(7); // boundary
        // Inclusive Boundary: 9223372036854775807
        assertThat(partitionForToken(new BigInteger("9223372036854775807"))).isEqualTo(7);
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
        assertThat(partitioner.numPartitions()).isEqualTo(4);
        assertThat(partitionForToken(new BigInteger("-9223372036854775807"))).isEqualTo(0);
        assertThat(partitionForToken(0)).isEqualTo(0);
        assertThat(partitionForToken(100000)).isEqualTo(1);
        assertThat(partitionForToken(100001)).isEqualTo(2);
        assertThat(partitionForToken(200001)).isEqualTo(3);
        assertThat(partitionForToken(new BigInteger("9223372036854775807"))).isEqualTo(3);
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
        assertThat(partitioner.numSplits()).isEqualTo(25);
        assertThat(partitioner.numPartitions()).isGreaterThanOrEqualTo(100);
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
        assertThat(partitioner.numSplits()).isEqualTo(50);
        assertThat(partitioner.numPartitions()).isGreaterThanOrEqualTo(200);
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
        assertThat(partitioner.numSplits()).isEqualTo(10);
        assertThat(partitioner.numPartitions()).isGreaterThanOrEqualTo(200);
    }

    // Range coverage validation must reject a partition map that leaves a token uncovered. The gap has to be
    // injected through a mocked mapping: TokenRangeMapping seeds its range map with the whole ring, so a mapping
    // built the normal way is always gap-free and cannot exercise the check.
    @Test
    public void testValidationDetectsRangeGap()
    {
        Partitioner ringPartitioner = Partitioner.Murmur3Partitioner;
        Range<BigInteger> wholeRing = Range.openClosed(ringPartitioner.minToken(), ringPartitioner.maxToken());
        // Take a gap-free split of the ring and punch a real, non-empty gap into the third sub-range by moving
        // its lower endpoint up, leaving (lowerEndpoint, lowerEndpoint + 10] uncovered
        List<Range<BigInteger>> rangesWithGap = new ArrayList<>(RangeUtils.split(wholeRing, 4));
        Range<BigInteger> third = rangesWithGap.get(2);
        BigInteger gapEnd = third.lowerEndpoint().add(BigInteger.TEN);
        rangesWithGap.set(2, Range.openClosed(gapEnd, third.upperEndpoint()));

        RangeMap<BigInteger, List<RingInstance>> gappedRangeMap = TreeRangeMap.create();
        rangesWithGap.forEach(range -> gappedRangeMap.put(range, Collections.emptyList()));

        @SuppressWarnings("unchecked")
        TokenRangeMapping<RingInstance> tokenRangeMapping = mock(TokenRangeMapping.class);
        when(tokenRangeMapping.partitioner()).thenReturn(ringPartitioner);
        when(tokenRangeMapping.getRangeMap()).thenReturn(gappedRangeMap);
        when(tokenRangeMapping.getTokenRanges()).thenReturn(ArrayListMultimap.create());

        // numberSplits of 1 leaves the ranges above untouched, so they reach the partition map as-is
        assertThatThrownBy(() -> new TokenPartitioner(tokenRangeMapping, 1, 2, 1, false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("There should be no missing ranges")
        .hasMessageContaining(String.format("(%s..%s]", third.lowerEndpoint(), gapEnd));
    }

    // Guards against over-correcting the fix for the above: the partition map is built from open-closed sub-ranges,
    // so minToken belongs to none of them. Validation that expected [minToken, maxToken] to be covered would report
    // a spurious [minToken, minToken] gap and fail every bulk write on a perfectly healthy ring.
    @Test
    public void testValidationAcceptsRingNotCoveringMinToken()
    {
        TokenRangeMapping<RingInstance> tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3), 3);
        partitioner = new TokenPartitioner(tokenRangeMapping, 2, 2, 1, false);
        BigInteger minToken = Partitioner.Murmur3Partitioner.minToken();
        assertThat(partitioner.getTokenRange(0).contains(minToken)).isFalse();
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
