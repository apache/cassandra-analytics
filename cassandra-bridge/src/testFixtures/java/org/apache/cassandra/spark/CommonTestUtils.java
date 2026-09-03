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

package org.apache.cassandra.spark;

import java.util.Random;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.test.QTRandom;
import org.quicktheories.core.Gen;

import static org.quicktheories.generators.SourceDSL.arbitrary;

// CHECKSTYLE IGNORE: default constructor is required as this util class is extended elsewhere
public class CommonTestUtils
{
    CommonTestUtils()
    {

    }

    public static Gen<CqlField.NativeType> cql3Type(CassandraBridge bridge)
    {
        return arbitrary().pick(bridge.supportedTypes());
    }

    /**
     * A {@link Random} whose entropy is drawn from QuickTheories' own seeded PRNG, so that any randomness
     * consumed via the returned instance is reproducible from the same QT seed as the rest of the property.
     */
    public static Gen<Random> qtRandom()
    {
        return QTRandom::new;
    }
}
