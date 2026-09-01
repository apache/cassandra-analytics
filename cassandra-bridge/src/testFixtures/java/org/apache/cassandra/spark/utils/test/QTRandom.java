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

import java.util.Random;

import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.Constraint;

/**
 * Bridges QuickTheories' seeded {@link RandomnessSource} into the standard {@link Random} API so that
 * existing {@code Random}-based test helpers (e.g. {@code RandomUtils}, {@code CqlType.randomValue}) draw
 * their entropy from QT's PRNG. Must only be used while the wrapped {@link RandomnessSource} is still live,
 * i.e. from within a {@code Gen#generate(RandomnessSource)} call - drawing from it afterwards is not safe
 * for QuickTheories' shrinking to replay.
 */
public final class QTRandom extends Random
{
    private final RandomnessSource source;

    public QTRandom(RandomnessSource source)
    {
        this.source = source;
    }

    @Override
    protected int next(int bits)
    {
        return (int) source.next(Constraint.between(0, (1L << bits) - 1));
    }
}
