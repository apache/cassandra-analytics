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

package org.apache.cassandra.spark.data;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.data.partitioner.CassandraInstance;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises token-owner hint selection for S3 reads. The hint is a secondary tie-breaker for
 * {@link PartitionedDataLayer#splitReplicas}; it must identify a deterministic owner from range
 * geometry without relying on replica-list order.
 */
class S3CassandraDataLayerPrimaryHintTest
{
    @Test
    void exactTokenMatchSelectsOwner()
    {
        CassandraInstance a = instance("100", "node-a");
        CassandraInstance b = instance("200", "node-b");
        CassandraInstance c = instance("300", "node-c");

        Predicate<CassandraInstance> hint = invokeGetPrimaryHint(
            Collections.singletonMap(rangeOf(100L, 200L), Arrays.asList(a, b, c)));

        assertThat(hint.test(b)).isTrue();
        assertThat(hint.test(a)).isFalse();
        assertThat(hint.test(c)).isFalse();
    }

    @Test
    void subRangeUpperEndpointBetweenTokensSelectsCeiling()
    {
        CassandraInstance a = instance("100", "node-a");
        CassandraInstance b = instance("200", "node-b");
        CassandraInstance c = instance("300", "node-c");

        Predicate<CassandraInstance> hint = invokeGetPrimaryHint(
            Collections.singletonMap(rangeOf(100L, 150L), Arrays.asList(a, b, c)));

        assertThat(hint.test(b)).isTrue();
        assertThat(hint.test(a)).isFalse();
        assertThat(hint.test(c)).isFalse();
    }

    @Test
    void wrapAroundUpperBeyondAllTokensSelectsSmallestTokenReplica()
    {
        CassandraInstance a = instance("100", "node-a");
        CassandraInstance b = instance("200", "node-b");
        CassandraInstance c = instance("300", "node-c");

        Predicate<CassandraInstance> hint = invokeGetPrimaryHint(
            Collections.singletonMap(rangeOf(300L, 400L), Arrays.asList(a, b, c)));

        assertThat(hint.test(a)).isTrue();
        assertThat(hint.test(b)).isFalse();
        assertThat(hint.test(c)).isFalse();
    }

    @Test
    void naturalPrimaryFilteredOutSelectsNextGreaterTokenReplica()
    {
        CassandraInstance a = instance("100", "node-a");
        CassandraInstance c = instance("300", "node-c");

        Predicate<CassandraInstance> hint = invokeGetPrimaryHint(
            Collections.singletonMap(rangeOf(100L, 200L), Arrays.asList(a, c)));

        assertThat(hint.test(c)).isTrue();
        assertThat(hint.test(a)).isFalse();
    }

    @Test
    void multipleRangesEachContributePrimaries()
    {
        CassandraInstance a = instance("100", "node-a");
        CassandraInstance b = instance("200", "node-b");
        CassandraInstance c = instance("300", "node-c");

        Map<Range<BigInteger>, List<CassandraInstance>> ranges = new LinkedHashMap<>();
        ranges.put(rangeOf(0L, 100L), Arrays.asList(a, b, c));     // owner -> a
        ranges.put(rangeOf(100L, 200L), Arrays.asList(a, b, c));   // owner -> b
        ranges.put(rangeOf(200L, 300L), Arrays.asList(a, b, c));   // owner -> c

        Predicate<CassandraInstance> hint = invokeGetPrimaryHint(ranges);
        assertThat(hint.test(a)).isTrue();
        assertThat(hint.test(b)).isTrue();
        assertThat(hint.test(c)).isTrue();
    }

    @Test
    void emptyOrNullTokenInstancesAreSkippedDefensively()
    {
        CassandraInstance bogus = instance(null, "node-bogus");
        CassandraInstance a = instance("100", "node-a");
        CassandraInstance b = instance("200", "node-b");

        Predicate<CassandraInstance> hint = invokeGetPrimaryHint(
            Collections.singletonMap(rangeOf(100L, 150L), Arrays.asList(bogus, a, b)));

        assertThat(hint.test(b)).isTrue();
        assertThat(hint.test(bogus)).isFalse();
        assertThat(hint.test(a)).isFalse();
    }

    private static CassandraInstance instance(String token, String node)
    {
        return new CassandraInstance(token, node, "usw2");
    }

    private static Range<BigInteger> rangeOf(long start, long end)
    {
        return Range.openClosed(BigInteger.valueOf(start), BigInteger.valueOf(end));
    }

    @SuppressWarnings("unchecked")
    private static Predicate<CassandraInstance> invokeGetPrimaryHint(Map<Range<BigInteger>, List<CassandraInstance>> ranges)
    {
        try
        {
            // The hint is a pure function of the range map; bypass constructor wiring for S3,
            // Cassandra bridge, and partition planning state.
            Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            sun.misc.Unsafe unsafe = (sun.misc.Unsafe) theUnsafe.get(null);
            S3CassandraDataLayer layer =
                (S3CassandraDataLayer) unsafe.allocateInstance(S3CassandraDataLayer.class);

            Method method = S3CassandraDataLayer.class.getDeclaredMethod("getPrimaryHint", Map.class);
            method.setAccessible(true);
            return (Predicate<CassandraInstance>) method.invoke(layer, ranges);
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException(e);
        }
    }
}
