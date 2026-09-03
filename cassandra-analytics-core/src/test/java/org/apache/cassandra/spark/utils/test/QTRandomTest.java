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
    public void sameSeedDrawProducesSameOutput()
    {
        Random first = new QTRandom(new FixedSequenceSource(42L));
        Random second = new QTRandom(new FixedSequenceSource(42L));

        for (int i = 0; i < 100; i++)
        {
            assertThat(second.nextLong()).isEqualTo(first.nextLong());
        }
    }

    @Test
    public void differentSeedDrawsProduceDifferentOutput()
    {
        Random first = new QTRandom(new FixedSequenceSource(1L));
        Random second = new QTRandom(new FixedSequenceSource(2L));

        assertThat(first.nextLong()).isNotEqualTo(second.nextLong());
    }

    @Test
    public void onlyDrawsFromTheSourceOnceRegardlessOfSubsequentUsage()
    {
        // exactly one value is consumed to seed the Random, however much randomness is drawn afterwards
        FixedSequenceSource source = new FixedSequenceSource(7L);
        Random random = new QTRandom(source);

        assertThat(source.remaining()).isEqualTo(0);

        for (int i = 0; i < 10_000; i++)
        {
            random.nextInt();
        }

        assertThat(source.remaining()).isEqualTo(0);
    }

    /**
     * A {@link RandomnessSource} that replays a fixed sequence of longs. Used to prove {@link QTRandom}
     * draws exactly one value from the underlying QuickTheories {@link RandomnessSource} to seed itself,
     * and never touches the source again - the property that keeps QT's shrink bookkeeping bounded no
     * matter how much randomness the test subsequently draws from the returned {@link Random}.
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
