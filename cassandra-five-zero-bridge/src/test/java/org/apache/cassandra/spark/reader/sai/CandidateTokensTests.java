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
package org.apache.cassandra.spark.reader.sai;

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.spark.reader.DataDbRange;
import org.assertj.core.groups.Tuple;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CandidateTokensTests
{
    @Test
    void testMurmur3BuilderDeduplicatesExactTokens()
    {
        CandidateTokens.Builder builder = CandidateTokens.builder(Murmur3Partitioner.instance);
        builder.add(BigInteger.TEN);
        builder.add(BigInteger.TEN);
        builder.add(BigInteger.valueOf(11));
        builder.add(BigInteger.valueOf(20));
        builder.add(BigInteger.valueOf(21));
        builder.add(BigInteger.valueOf(23));

        CandidateTokens tokens = builder.build();

        assertThat(tokens.size()).isEqualTo(5);
        assertThat(tokens.contains(BigInteger.TEN)).isTrue();
        assertThat(tokens.contains(BigInteger.valueOf(12))).isFalse();
        assertThat(tokens.contains(BigInteger.valueOf(23))).isTrue();
    }

    @Test
    void testSliceUsesSortedExactTokens()
    {
        CandidateTokens.Builder builder = CandidateTokens.builder(Murmur3Partitioner.instance);
        builder.add(BigInteger.ONE);
        builder.add(BigInteger.TEN);
        builder.add(BigInteger.valueOf(20));
        builder.add(BigInteger.valueOf(30));

        CandidateTokens.Slice slice = builder.build().slice(BigInteger.valueOf(9), BigInteger.valueOf(21));

        assertThat(slice.size()).isEqualTo(2);
        assertThat(slice.tokenAt(0)).isEqualTo(BigInteger.TEN);
        assertThat(slice.tokenAt(1)).isEqualTo(BigInteger.valueOf(20));
    }

    @Test
    void testRandomPartitionerUsesBigIntegerFallback()
    {
        CandidateTokens.Builder builder = CandidateTokens.builder(RandomPartitioner.instance);
        BigInteger large = BigInteger.ONE.shiftLeft(100);
        builder.add(BigInteger.ONE);
        builder.add(large);

        CandidateTokens tokens = builder.build();

        assertThat(tokens.size()).isEqualTo(2);
        assertThat(tokens.contains(large)).isTrue();
        assertThat(tokens.slice(BigInteger.TEN, large).tokenAt(0)).isEqualTo(large);
    }

    @Test
    void testBuilderRejectsOutOfOrderTokens()
    {
        CandidateTokens.Builder builder = CandidateTokens.builder(Murmur3Partitioner.instance);
        builder.add(BigInteger.TEN);

        assertThatThrownBy(() -> builder.add(BigInteger.ONE)).hasMessageContaining("sorted order");
    }

    @Test
    void testDataDbAccumulatorMergesByPhysicalGap()
    {
        DataDbRange.Accumulator accumulator = new DataDbRange.Accumulator(10L);
        accumulator.add(10, 20);
        accumulator.add(30, 40);
        accumulator.add(60, 70);

        assertThat(accumulator.build())
        .extracting(DataDbRange::start, DataDbRange::end)
        .containsExactly(Tuple.tuple(10L, 40L),
                         Tuple.tuple(60L, 70L));
    }
}
