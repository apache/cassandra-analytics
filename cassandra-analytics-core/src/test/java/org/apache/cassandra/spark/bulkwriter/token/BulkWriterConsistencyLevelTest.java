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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    "dc1", "3",
    "dc2", "3"));

    private static List<CassandraInstance> succeededNone = Collections.emptyList();
    private static List<CassandraInstance> succeededOne;
    private static List<CassandraInstance> succeededOnePerDc;
    private static List<CassandraInstance> succeededTwo;
    private static List<CassandraInstance> succeededTwoPerDc;
    private static List<CassandraInstance> succeededThree;
    private static List<CassandraInstance> succeededThreePerDc;
    private static List<CassandraInstance> succeededQuorum; // across DC
    private static List<CassandraInstance> succeededHalf; // across DC; half < quorum
    private static List<CassandraInstance> succeededAll; // all the nodes

    private static Set<CassandraInstance> zero = Collections.emptySet();
    private static Set<CassandraInstance> onePerDc = intToSet(1);
    private static Set<CassandraInstance> one = intToSet(1, true);
    private static Set<CassandraInstance> twoPerDc = intToSet(2);
    private static Set<CassandraInstance> two = intToSet(2, true);
    private static Set<CassandraInstance> threePerDc = intToSet(3);
    private static Set<CassandraInstance> three = intToSet(3, true);

    @BeforeAll
    static void setup()
    {
        CassandraInstance dc1i1 = mockInstance("dc1");
        CassandraInstance dc1i2 = mockInstance("dc1");
        CassandraInstance dc1i3 = mockInstance("dc1");
        CassandraInstance dc2i1 = mockInstance("dc2");
        CassandraInstance dc2i2 = mockInstance("dc2");
        CassandraInstance dc2i3 = mockInstance("dc2");
        succeededOne = Arrays.asList(dc1i1);
        succeededTwo = Arrays.asList(dc1i1, dc1i2);
        succeededThree = Arrays.asList(dc1i1, dc1i2, dc1i3);
        succeededOnePerDc = Arrays.asList(dc1i1,
                                          dc2i1);
        succeededTwoPerDc = Arrays.asList(dc1i1, dc1i2,
                                          dc2i1, dc2i2);
        succeededThreePerDc = Arrays.asList(dc1i1, dc1i2, dc1i3,
                                            dc2i1, dc2i2, dc2i3);
        succeededQuorum = Arrays.asList(dc1i1, dc1i2, dc1i3,
                                        dc2i1);
        succeededHalf = Arrays.asList(dc1i1, dc1i2,
                                      dc2i1);
        succeededAll = succeededThreePerDc;
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

        testCanBeSatisfied(CL.EACH_QUORUM, succeededTwoPerDc, true);
        testCanBeSatisfied(CL.EACH_QUORUM, succeededThreePerDc, true);

        testCanBeSatisfied(CL.QUORUM, succeededTwoPerDc, true);
        testCanBeSatisfied(CL.QUORUM, succeededThreePerDc, true);
        testCanBeSatisfied(CL.QUORUM, succeededQuorum, true);

        testCanBeSatisfied(CL.ALL, succeededAll, true);
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
        testCanBeSatisfied(CL.EACH_QUORUM, succeededOnePerDc, false);

        testCanBeSatisfied(CL.QUORUM, succeededNone, false);
        testCanBeSatisfied(CL.QUORUM, succeededOne, false);
        testCanBeSatisfied(CL.QUORUM, succeededOnePerDc, false);
        testCanBeSatisfied(CL.QUORUM, succeededTwo, false);
        testCanBeSatisfied(CL.QUORUM, succeededHalf, false);

        testCanBeSatisfied(CL.ALL, succeededNone, false);
        testCanBeSatisfied(CL.ALL, succeededOne, false);
        testCanBeSatisfied(CL.ALL, succeededOnePerDc, false);
        testCanBeSatisfied(CL.ALL, succeededTwo, false);
        testCanBeSatisfied(CL.ALL, succeededTwoPerDc, false);
        testCanBeSatisfied(CL.ALL, succeededQuorum, false);
        testCanBeSatisfied(CL.ALL, succeededHalf, false);
    }

    @Test
    void testCanBeSatisfiedWithPendingReturnsTrue()
    {
        testCanBeSatisfiedWithPending(CL.ONE, /* succeeded */ three, /* pending */ zero, true);
        testCanBeSatisfiedWithPending(CL.ONE, /* succeeded */ three, /* pending */ two, true);
        testCanBeSatisfiedWithPending(CL.ONE, /* succeeded */ three, /* pending */ one, true);
        testCanBeSatisfiedWithPending(CL.ONE, /* succeeded */ two, /* pending */ zero, true);
        testCanBeSatisfiedWithPending(CL.ONE, /* succeeded */ two, /* pending */ one, true);
        testCanBeSatisfiedWithPending(CL.ONE, /* succeeded */ one, /* pending */ zero, true);

        testCanBeSatisfiedWithPending(CL.TWO, /* succeeded */ three, /* pending */ zero, true);
        testCanBeSatisfiedWithPending(CL.TWO, /* succeeded */ three, /* pending */ one, true);
        testCanBeSatisfiedWithPending(CL.TWO, /* succeeded */ two, /* pending */ zero, true);

        testCanBeSatisfiedWithPending(CL.LOCAL_ONE, /* succeeded */ three, /* pending */ zero, true);
        testCanBeSatisfiedWithPending(CL.LOCAL_ONE, /* succeeded */ three, /* pending */ two, true);
        testCanBeSatisfiedWithPending(CL.LOCAL_ONE, /* succeeded */ three, /* pending */ one, true);
        testCanBeSatisfiedWithPending(CL.LOCAL_ONE, /* succeeded */ two, /* pending */ zero, true);
        testCanBeSatisfiedWithPending(CL.LOCAL_ONE, /* succeeded */ two, /* pending */ one, true);
        testCanBeSatisfiedWithPending(CL.LOCAL_ONE, /* succeeded */ one, /* pending */ zero, true);

        testCanBeSatisfiedWithPending(CL.LOCAL_QUORUM, /* succeeded */ three, /* pending */ zero, true);
        testCanBeSatisfiedWithPending(CL.LOCAL_QUORUM, /* succeeded */ three, /* pending */ one, true);
        testCanBeSatisfiedWithPending(CL.LOCAL_QUORUM, /* succeeded */ three, /* pending */ onePerDc, true);
        testCanBeSatisfiedWithPending(CL.LOCAL_QUORUM, /* succeeded */ two, /* pending */ zero, true);

        testCanBeSatisfiedWithPending(CL.EACH_QUORUM, /* succeeded */ threePerDc, /* pending */ zero, true);
        testCanBeSatisfiedWithPending(CL.EACH_QUORUM, /* succeeded */ threePerDc, /* pending */ one, true);
        testCanBeSatisfiedWithPending(CL.EACH_QUORUM, /* succeeded */ threePerDc, /* pending */ onePerDc, true);
        testCanBeSatisfiedWithPending(CL.EACH_QUORUM, /* succeeded */ twoPerDc, /* pending */ zero, true);

        testCanBeSatisfiedWithPending(CL.QUORUM, /* succeeded */ threePerDc, /* pending */ zero, true);
        testCanBeSatisfiedWithPending(CL.QUORUM, /* succeeded */ threePerDc, /* pending */ onePerDc, true);
        testCanBeSatisfiedWithPending(CL.QUORUM, /* succeeded */ twoPerDc, /* pending */ zero, true);

        testCanBeSatisfiedWithPending(CL.ALL, /* succeeded */ threePerDc, /* pending */ zero, true);
    }

    @Test
    void testCanBeSatisfiedWithPendingReturnsFalse()
    {
        testCanBeSatisfiedWithPending(CL.ONE, /* succeeded */ zero, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.ONE, /* succeeded */ onePerDc, /* pending */ onePerDc, false);

        testCanBeSatisfiedWithPending(CL.TWO, /* succeeded */ zero, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.TWO, /* succeeded */ one, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.TWO, /* succeeded */ two, /* pending */ one, false);

        testCanBeSatisfiedWithPending(CL.LOCAL_ONE, /* succeeded */ zero, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.LOCAL_ONE, /* succeeded */ one, /* pending */ one, false);
        testCanBeSatisfiedWithPending(CL.LOCAL_ONE, /* succeeded */ two, /* pending */ two, false);

        testCanBeSatisfiedWithPending(CL.LOCAL_QUORUM, /* succeeded */ zero, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.LOCAL_QUORUM, /* succeeded */ one, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.LOCAL_QUORUM, /* succeeded */ two, /* pending */ one, false);

        testCanBeSatisfiedWithPending(CL.EACH_QUORUM, /* succeeded */ zero, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.EACH_QUORUM, /* succeeded */ onePerDc, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.EACH_QUORUM, /* succeeded */ twoPerDc, /* pending */ onePerDc, false);

        testCanBeSatisfiedWithPending(CL.QUORUM, /* succeeded */ zero, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.QUORUM, /* succeeded */ onePerDc, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.QUORUM, /* succeeded */ twoPerDc, /* pending */ onePerDc, false);

        testCanBeSatisfiedWithPending(CL.ALL, /* succeeded */ two, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.ALL, /* succeeded */ twoPerDc, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.ALL, /* succeeded */ one, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.ALL, /* succeeded */ onePerDc, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.ALL, /* succeeded */ zero, /* pending */ zero, false);
        testCanBeSatisfiedWithPending(CL.ALL, /* succeeded */ threePerDc, /* pending */ one, false);
        testCanBeSatisfiedWithPending(CL.ALL, /* succeeded */ threePerDc, /* pending */ onePerDc, false);
    }

    private void testCanBeSatisfied(ConsistencyLevel cl, List<CassandraInstance> succeeded, boolean expectedResult)
    {
        assertThat(cl.canBeSatisfied(succeeded, zero, replicationFactor, "dc1")).isEqualTo(expectedResult);
    }

    private void testCanBeSatisfiedWithPending(ConsistencyLevel cl,
                                               Set<CassandraInstance> succeeded,
                                               Set<CassandraInstance> pending,
                                               boolean expectedResult)
    {
        assertThat(cl.canBeSatisfied(succeeded, pending, replicationFactor, "dc1")).isEqualTo(expectedResult);
    }

    private static CassandraInstance mockInstance(String dc)
    {
        CassandraInstance i = mock(CassandraInstance.class);
        when(i.datacenter()).thenReturn(dc);
        return i;
    }

    private static Set<CassandraInstance> intToSet(int i)
    {
        return intToSet(i, false);
    }

    private static Set<CassandraInstance> intToSet(int i, boolean singleDc)
    {
        Set<CassandraInstance> res = new HashSet<>();
        for (int j = 0; j < i; j++)
        {
            CassandraInstance dc1Instance = mock(CassandraInstance.class);
            when(dc1Instance.datacenter()).thenReturn("dc1");
            res.add(dc1Instance);
            if (singleDc)
            {
                continue;
            }
            CassandraInstance dc2Instance = mock(CassandraInstance.class);
            when(dc2Instance.datacenter()).thenReturn("dc2");
            res.add(dc2Instance);
        }
        return res;
    }
}
