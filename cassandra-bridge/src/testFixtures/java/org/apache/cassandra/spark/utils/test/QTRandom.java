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
 * A {@link Random} seeded from QuickTheories' PRNG, so tests using it are reproducible from QT's seed.
 * Draws one seed value from the {@link RandomnessSource} and behaves as an ordinary {@link Random}
 * afterwards - QT records every draw from a {@link RandomnessSource} for shrinking, so forwarding all
 * calls to it would grow unbounded on data-heavy tests.
 */
public final class QTRandom extends Random
{
    public QTRandom(RandomnessSource source)
    {
        super(source.next(Constraint.between(Long.MIN_VALUE, Long.MAX_VALUE)));
    }
}

