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

package org.apache.cassandra.spark.bulkwriter.token;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;

import static org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel.CL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BulkWriterConsistencyLevelTest
{
    private static final ReplicationFactor replicationFactor = new ReplicationFactor(ImmutableMap.of(
    "class", "NetworkTopologyStrategy",
    "dc1", "3"));

    private static List<CassandraInstance> succeededNone = Collections.emptyList();
    private static List<CassandraInstance> succeededOne;
    private static List<CassandraInstance> succeededTwo;
    private static List<CassandraInstance> succeededThree;

    @BeforeAll
    static void setup()
    {
        CassandraInstance i1 = mockInstance("dc1");
        CassandraInstance i2 = mockInstance("dc1");
        CassandraInstance i3 = mockInstance("dc1");
        succeededOne = Arrays.asList(i1);
        succeededTwo = Arrays.asList(i1, i2);
        succeededThree = Arrays.asList(i1, i2, i3);
    }

    @Test
    void testCanBeSatisfiedReturnsTrue()
    {
        testCanBeSatisfied(CL.ONE, succeededOne, true);
        testCanBeSatisfied(CL.ONE, succeededTwo, true);
        testCanBeSatisfied(CL.ONE, succeededThree, true);

        testCanBeSatisfied(CL.TWO, succeededTwo, true);
        testCanBeSatisfied(CL.TWO, succeededThree, true);

        testCanBeSatisfied(CL.LOCAL_ONE, succeededOne, true);
        testCanBeSatisfied(CL.LOCAL_ONE, succeededTwo, true);
        testCanBeSatisfied(CL.LOCAL_ONE, succeededThree, true);

        testCanBeSatisfied(CL.LOCAL_QUORUM, succeededTwo, true);
        testCanBeSatisfied(CL.LOCAL_QUORUM, succeededThree, true);

        testCanBeSatisfied(CL.EACH_QUORUM, succeededTwo, true);
        testCanBeSatisfied(CL.EACH_QUORUM, succeededThree, true);

        testCanBeSatisfied(CL.QUORUM, succeededTwo, true);
        testCanBeSatisfied(CL.QUORUM, succeededThree, true);

        testCanBeSatisfied(CL.ALL, succeededThree, true);
    }

    @Test
    void testCanBeSatisfiedReturnsFalse()
    {
        testCanBeSatisfied(CL.ONE, succeededNone, false);

        testCanBeSatisfied(CL.TWO, succeededNone, false);
        testCanBeSatisfied(CL.TWO, succeededOne, false);

        testCanBeSatisfied(CL.LOCAL_ONE, succeededNone, false);

        testCanBeSatisfied(CL.LOCAL_QUORUM, succeededNone, false);
        testCanBeSatisfied(CL.LOCAL_QUORUM, succeededOne, false);

        testCanBeSatisfied(CL.EACH_QUORUM, succeededNone, false);
        testCanBeSatisfied(CL.EACH_QUORUM, succeededOne, false);

        testCanBeSatisfied(CL.QUORUM, succeededNone, false);
        testCanBeSatisfied(CL.QUORUM, succeededOne, false);

        testCanBeSatisfied(CL.ALL, succeededNone, false);
        testCanBeSatisfied(CL.ALL, succeededOne, false);
        testCanBeSatisfied(CL.ALL, succeededTwo, false);
    }

    private void testCanBeSatisfied(ConsistencyLevel cl, List<CassandraInstance> succeeded, boolean expectedResult)
    {
        assertThat(cl.canBeSatisfied(succeeded, replicationFactor, "dc1")).isEqualTo(expectedResult);
    }

    private static CassandraInstance mockInstance(String dc)
    {
        CassandraInstance i = mock(CassandraInstance.class);
        when(i.datacenter()).thenReturn(dc);
        return i;
    }
}
