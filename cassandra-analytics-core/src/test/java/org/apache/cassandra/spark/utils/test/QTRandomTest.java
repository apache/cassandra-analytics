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

package org.apache.cassandra.spark.utils.test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Random;

import org.junit.jupiter.api.Test;

import org.quicktheories.core.DetatchedRandomnessSource;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.Constraint;

import static org.assertj.core.api.Assertions.assertThat;

public class QTRandomTest
{
    @Test
    public void sameUnderlyingDrawsProduceSameOutput()
    {
        long[] draws = {42L, 1337L, -7L, 999999L, 0L};
        Random first = new QTRandom(new FixedSequenceSource(draws));
        Random second = new QTRandom(new FixedSequenceSource(draws));

        for (int i = 0; i < draws.length; i++)
        {
            assertThat(second.nextInt()).isEqualTo(first.nextInt());
        }
    }

    @Test
    public void differentUnderlyingDrawsProduceDifferentOutput()
    {
        Random first = new QTRandom(new FixedSequenceSource(1L, 2L, 3L, 4L));
        Random second = new QTRandom(new FixedSequenceSource(5L, 6L, 7L, 8L));

        assertThat(first.nextInt()).isNotEqualTo(second.nextInt());
    }

    @Test
    public void delegatesEveryDrawToTheUnderlyingSource()
    {
        // java.util.Random#nextLong() draws exactly two 32-bit values via next(int)
        FixedSequenceSource source = new FixedSequenceSource(0L, 1L);
        Random random = new QTRandom(source);

        random.nextLong();

        assertThat(source.remaining()).isEqualTo(0);
    }

    /**
     * A {@link RandomnessSource} that replays a fixed sequence of longs, one per {@code next(bits)} call
     * made by {@link QTRandom}. Used to prove {@link QTRandom} is a pure function of the underlying
     * QuickTheories {@link RandomnessSource} - the same sequence in always produces the same output out.
     */
    private static final class FixedSequenceSource implements RandomnessSource
    {
        private final Deque<Long> values;

        FixedSequenceSource(long... values)
        {
            this.values = new ArrayDeque<>();
            for (long value : values)
            {
                this.values.add(value);
            }
        }

        @Override
        public long next(Constraint constraints)
        {
            return values.poll();
        }

        @Override
        public DetatchedRandomnessSource detach()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerFailedAssumption()
        {
        }

        int remaining()
        {
            return values.size();
        }
    }
}
