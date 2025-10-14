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

package org.apache.cassandra.spark.data.converter.types;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.spark.data.complex.CqlList;
import org.apache.cassandra.spark.data.complex.CqlVector;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorTypeTests
{
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();

    @Test
    public void testSimpleTypeConversion()
    {
        CqlVector cqlVector = new CqlVector(org.apache.cassandra.spark.data.types.Float.INSTANCE, 3);
        Object cqlWriterObj = cqlVector.convertForCqlWriter(List.of(3.14f, 0.0f, -1f), BRIDGE.getVersion(), false);
        assertThat(cqlWriterObj).isInstanceOf(List.class);
        List<Float> cqlWriterList = (List<Float>) cqlWriterObj;
        assertThat(cqlWriterList).containsExactly(3.14f, 0.0f, -1f);
    }

    @Test
    public void testComplexTypeConversion()
    {
        CqlVector cqlVector = new CqlVector(CqlList.set(org.apache.cassandra.spark.data.types.Float.INSTANCE), 3);
        Object cqlWriterObj = cqlVector.convertForCqlWriter(List.of(Set.of(3.14f, 0f), Set.of(1f), Set.of()), BRIDGE.getVersion(), false);
        assertThat(cqlWriterObj).isInstanceOf(List.class);
        List<? extends Set<Float>> cqlWriterList = (List<? extends Set<Float>>) cqlWriterObj;
        assertThat(cqlWriterList).hasSize(3);
        assertThat(cqlWriterList.get(0)).containsExactlyInAnyOrder(3.14f, 0f);
        assertThat(cqlWriterList.get(1)).containsExactly(1f);
        assertThat(cqlWriterList.get(2)).isEmpty();
    }
}
