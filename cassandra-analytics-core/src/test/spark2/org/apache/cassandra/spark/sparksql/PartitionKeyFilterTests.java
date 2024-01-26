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

package org.apache.cassandra.spark.sparksql;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.reader.SparkSSTableReader;
import org.apache.cassandra.spark.sparksql.filters.PartitionKeyFilter;
import org.apache.cassandra.spark.utils.RangeUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class PartitionKeyFilterTests
{
    @Test
    public void testValidFilter()
    {
        qt().forAll(TestUtils.bridges())
            .checkAssert(bridge -> {
                ByteBuffer key = bridge.aInt().serialize(10);
                BigInteger token = bridge.hash(Partitioner.Murmur3Partitioner, key);
                PartitionKeyFilter filter = PartitionKeyFilter.create(key, token);

                ByteBuffer diffKey = bridge.aInt().serialize(11);
                TokenRange inRange = TokenRange.singleton(token);
                TokenRange notInRange = TokenRange.singleton(token.subtract(BigInteger.ONE));
                SparkSSTableReader reader = mock(SparkSSTableReader.class);
                when(reader.range()).thenReturn(TokenRange.singleton(token));

                assertTrue(filter.filter(key));
                assertFalse(filter.filter(diffKey));
                assertTrue(filter.overlaps(inRange));
                assertFalse(filter.overlaps(notInRange));
                assertTrue(filter.matches(key));
                assertFalse(filter.matches(diffKey));
                assertTrue(SparkSSTableReader.overlaps(reader, filter.tokenRange()));
            });
    }

    @Test
    public void testEmptyKey()
    {
        assertThrows(IllegalArgumentException.class, () ->
                                                     PartitionKeyFilter.create(ByteBuffer.wrap(new byte[0]),
                                                                               BigInteger.ZERO));
    }

    @Test
    public void testTokenRing()
    {
        qt().forAll(TestUtils.bridges(), TestUtils.partitioners(), arbitrary().pick(Arrays.asList(1, 3, 6, 12, 128)))
            .checkAssert((bridge, partitioner, numInstances) -> {
                CassandraRing ring = TestUtils.createRing(partitioner, numInstances);
                TokenPartitioner tokenPartitioner = new TokenPartitioner(ring, 24, 24);
                List<BigInteger> boundaryTokens = IntStream.range(0, tokenPartitioner.numPartitions())
                                                           .mapToObj(tokenPartitioner::getTokenRange)
                                                           .map(range -> Arrays.asList(range.lowerEndpoint(),
                                                                                       midPoint(range),
                                                                                       range.upperEndpoint()))
                                                           .flatMap(Collection::stream)
                                                           .collect(Collectors.toList());
                for (BigInteger token : boundaryTokens)
                {
                    if (token.equals(partitioner.minToken()))
                    {
                        // minToken is excluded in the ring
                        continue;
                    }
                    // Check boundary tokens only match 1 Spark token range
                    PartitionKeyFilter filter = PartitionKeyFilter.create(bridge.aInt().serialize(11), token);
                    assertEquals(1, tokenPartitioner.subRanges().stream()
                                                    .map(RangeUtils::toTokenRange)
                                                    .filter(filter::overlaps)
                                                    .count());
                }
            });
    }

    private static BigInteger midPoint(Range<BigInteger> range)
    {
        return range.upperEndpoint()
                    .subtract(range.lowerEndpoint())
                    .divide(BigInteger.valueOf(2L))
                    .add(range.lowerEndpoint());
    }
}
