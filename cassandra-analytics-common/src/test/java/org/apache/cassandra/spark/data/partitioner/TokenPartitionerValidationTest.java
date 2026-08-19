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
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.utils.RangeUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TokenPartitionerValidationTest
{
    private static final Partitioner PARTITIONER = Partitioner.Murmur3Partitioner;

    @Test
    public void testValidationDetectsRangeGap()
    {
        // Take a gap-free split of the ring and punch a real, non-empty gap into the third sub-range by
        // moving its lower endpoint up, leaving (lowerEndpoint, lowerEndpoint + 10] uncovered
        List<Range<BigInteger>> subRangesWithGap = new ArrayList<>(RangeUtils.split(wholeRing(), 4));
        Range<BigInteger> third = subRangesWithGap.get(2);
        BigInteger gapEnd = third.lowerEndpoint().add(BigInteger.TEN);
        subRangesWithGap.set(2, Range.openClosed(gapEnd, third.upperEndpoint()));

        assertThatThrownBy(() -> new TokenPartitioner(subRangesWithGap, ring()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("There should be no missing ranges")
        .hasMessageContaining(String.format("(%s..%s]", third.lowerEndpoint(), gapEnd));
    }

    @Test
    public void testValidationAcceptsCompleteRangeCoverage()
    {
        // minToken is deliberately left uncovered: the sub-ranges are open-closed, so it belongs to none of them.
        // Validation must not report it as a gap, otherwise every job fails on a healthy ring.
        TokenPartitioner partitioner = new TokenPartitioner(RangeUtils.split(wholeRing(), 4), ring());
        assertThat(partitioner.numPartitions()).isEqualTo(4);
    }

    private static Range<BigInteger> wholeRing()
    {
        return Range.openClosed(PARTITIONER.minToken(), PARTITIONER.maxToken());
    }

    private static CassandraRing ring()
    {
        List<CassandraInstance> instances = Arrays.asList(new CassandraInstance("0", "local0-i1", "DEV"),
                                                          new CassandraInstance("100", "local0-i2", "DEV"),
                                                          new CassandraInstance("200", "local0-i3", "DEV"));
        return new CassandraRing(PARTITIONER,
                                 "test",
                                 new ReplicationFactor(ImmutableMap.of("class", "NetworkTopologyStrategy", "DEV", "3")),
                                 instances);
    }
}
