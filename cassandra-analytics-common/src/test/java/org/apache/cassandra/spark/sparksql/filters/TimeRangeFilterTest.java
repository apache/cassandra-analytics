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

package org.apache.cassandra.spark.sparksql.filters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link TimeRangeFilter}
 */
class TimeRangeFilterTest
{
    @Test
    void testCreation()
    {
        TimeRangeFilter filter = TimeRangeFilter.create(100L, 200L);
        assertThat(filter.range().hasLowerBound()).isTrue();
        assertThat(filter.range().hasUpperBound()).isTrue();
        assertThat(filter.range().lowerEndpoint()).isEqualTo(100);
        assertThat(filter.range().upperEndpoint()).isEqualTo(200);
    }

    @Test
    void testThrowsExceptionWhenStartGreaterThanEnd()
    {
        assertThatThrownBy(() -> TimeRangeFilter.create(200L, 100L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid range: [200‥100]");
    }

    @Test
    void testFiltering()
    {
        // both start and end inclusive

        TimeRangeFilter sstableBeforeInclusiveBounds = TimeRangeFilter.create(100L, 200L);
        // SSTable: [0, 50], Filter: [100, 200]
        assertThat(sstableBeforeInclusiveBounds.filter(0L, 50L)).isFalse();

        TimeRangeFilter sstableTouchingStartInclusiveBound = TimeRangeFilter.create(100L, 200L);
        // SSTable: [0, 100], Filter: [100, 200] - touching inclusive
        assertThat(sstableTouchingStartInclusiveBound.filter(0L, 100L)).isTrue();

        TimeRangeFilter sstableAfterInclusiveBounds = TimeRangeFilter.create(100L, 200L);
        // SSTable: [250, 300], Filter: [100, 200]
        assertThat(sstableAfterInclusiveBounds.filter(250L, 300L)).isFalse();

        TimeRangeFilter sstableTouchingEndInclusiveBounds = TimeRangeFilter.create(100L, 200L);
        // SSTable: [200, 300], Filter: [100, 200] - touching inclusive
        assertThat(sstableTouchingEndInclusiveBounds.filter(200L, 300L)).isTrue();

        TimeRangeFilter sstableWithinInclusiveBounds = TimeRangeFilter.create(100L, 200L);
        // SSTable: [120, 180], Filter: [100, 200]
        assertThat(sstableWithinInclusiveBounds.filter(120L, 180L)).isTrue();

        TimeRangeFilter sstableEndOverlapInclusiveBounds = TimeRangeFilter.create(100L, 200L);
        // SSTable: [50, 150], Filter: [100, 200]
        assertThat(sstableEndOverlapInclusiveBounds.filter(50L, 150L)).isTrue();

        TimeRangeFilter sstableStartOverlapInclusiveBounds = TimeRangeFilter.create(100L, 200L);
        // SSTable: [150, 250], Filter: [100, 200]
        assertThat(sstableStartOverlapInclusiveBounds.filter(150L, 250L)).isTrue();

        TimeRangeFilter sstableTimeRangeLargerThanInclusiveBounds = TimeRangeFilter.create(100L, 200L);
        // SSTable: [50, 250], Filter: [100, 200]
        assertThat(sstableTimeRangeLargerThanInclusiveBounds.filter(50L, 250L)).isTrue();

        // end inclusive

        TimeRangeFilter sstableTouchingStartExclusive = TimeRangeFilter.create(101L, 200L);
        // SSTable: [0, 100], Filter: (100, 200] - no overlap
        assertThat(sstableTouchingStartExclusive.filter(0L, 100L)).isFalse();

        TimeRangeFilter sstableOverlapWithStartExclusive = TimeRangeFilter.create(99L, 200L);
        // SSTable: [50, 150], Filter: (100, 200]
        assertThat(sstableOverlapWithStartExclusive.filter(50L, 150L)).isTrue();

        // start inclusive

        TimeRangeFilter sstableTouchingEndExclusive = TimeRangeFilter.create(100L, 199L);
        // SSTable: [200, 300], Filter: [100, 200) - no overlap
        assertThat(sstableTouchingEndExclusive.filter(200L, 300L)).isFalse();

        TimeRangeFilter sstableOverlapEndExclusive = TimeRangeFilter.create(100L, 199L);
        // SSTable: [150, 250], Filter: [100, 200)
        assertThat(sstableOverlapEndExclusive.filter(150L, 250L)).isTrue();

        // both start and end exclusive

        TimeRangeFilter startEndExclusiveTouchingStart = TimeRangeFilter.create(101L, 199L);
        // SSTable: [0, 100], Filter: (100, 200)
        assertThat(startEndExclusiveTouchingStart.filter(0L, 100L)).isFalse();

        TimeRangeFilter startEndExclusiveTouchingEnd = TimeRangeFilter.create(99L, 199L);
        // SSTable: [200, 300], Filter: (100, 200)
        assertThat(startEndExclusiveTouchingEnd.filter(200L, 300L)).isFalse();

        TimeRangeFilter startEndExclusiveEnclosed = TimeRangeFilter.create(99L, 199L);
        // SSTable: [120, 180], Filter: (100, 200)
        assertThat(startEndExclusiveEnclosed.filter(120L, 180L)).isTrue();
    }

    @Test
    void testStartingAtInclusive()
    {
        TimeRangeFilter filter = TimeRangeFilter.startingAt(100L);
        assertThat(filter.range().hasLowerBound()).isTrue();
        assertThat(filter.range().lowerEndpoint()).isEqualTo(100L);
        assertThat(filter.range().hasUpperBound()).isFalse();

        assertThat(filter.filter(0L, 50L)).isFalse();
        assertThat(filter.filter(0L, 100L)).isTrue();
        assertThat(filter.filter(100L, 200L)).isTrue();
        assertThat(filter.filter(200L, 300L)).isTrue();
    }

    @Test
    void testStartingAtExclusive()
    {
        TimeRangeFilter filter = TimeRangeFilter.startingAt(101L);
        assertThat(filter.range().hasLowerBound()).isTrue();
        assertThat(filter.range().lowerEndpoint()).isEqualTo(101);
        assertThat(filter.range().hasUpperBound()).isFalse();

        assertThat(filter.filter(0L, 100L)).isFalse();
        assertThat(filter.filter(100L, 200L)).isTrue();
    }

    @Test
    void testEndingAtInclusive()
    {
        TimeRangeFilter filter = TimeRangeFilter.endingAt(200L);
        assertThat(filter.range().hasLowerBound()).isFalse();
        assertThat(filter.range().upperEndpoint()).isEqualTo(200);
        assertThat(filter.range().hasUpperBound()).isTrue();

        assertThat(filter.filter(0L, 100L)).isTrue();
        assertThat(filter.filter(100L, 200L)).isTrue();
        assertThat(filter.filter(200L, 300L)).isTrue();
        assertThat(filter.filter(250L, 300L)).isFalse();
    }

    @Test
    void testEndingAtExclusive()
    {
        TimeRangeFilter filter = TimeRangeFilter.endingAt(199L);
        assertThat(filter.range().hasLowerBound()).isFalse();
        assertThat(filter.range().upperEndpoint()).isEqualTo(199L);
        assertThat(filter.range().hasUpperBound()).isTrue();

        assertThat(filter.filter(100L, 200L)).isTrue();
        assertThat(filter.filter(200L, 300L)).isFalse();
    }

    @Test
    void testToString()
    {
        TimeRangeFilter boundedRangeFilter = TimeRangeFilter.create(100L, 200L);
        assertThat(boundedRangeFilter.toString()).isEqualTo("TimeRangeFilter[100‥200]");

        TimeRangeFilter startAtInclusiveFilter = TimeRangeFilter.startingAt(100L);
        assertThat(startAtInclusiveFilter.toString()).isEqualTo("TimeRangeFilter[100‥+∞)");

        TimeRangeFilter endAtInclusiveFilter = TimeRangeFilter.endingAt(200L);
        assertThat(endAtInclusiveFilter.toString()).isEqualTo("TimeRangeFilter(-∞‥200]");
    }

    @Test
    void testEquals()
    {
        TimeRangeFilter filter1 = TimeRangeFilter.create(100L, 200L);
        TimeRangeFilter filter2 = TimeRangeFilter.create(100L, 200L);
        TimeRangeFilter filter4 = TimeRangeFilter.create(100L, 300L);

        assertThat(filter1).isEqualTo(filter2);
        assertThat(filter1).isNotEqualTo(filter4);
    }

    @Test
    void testHashCode()
    {
        TimeRangeFilter filter1 = TimeRangeFilter.create(100L, 200L);
        TimeRangeFilter filter2 = TimeRangeFilter.create(100L, 200L);
        TimeRangeFilter filter3 = TimeRangeFilter.create(100L, 300L);

        assertThat(filter1.hashCode()).isEqualTo(filter2.hashCode());
        assertThat(filter1.hashCode()).isNotEqualTo(filter3.hashCode());
    }

    @Test
    void testStartEndSerialization() throws Exception
    {
        TimeRangeFilter original = TimeRangeFilter.create(100L, 200L);
        ByteArrayOutputStream baos = serialize(original);
        TimeRangeFilter deserialized = deserialize(baos);
        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.range().hasLowerBound()).isTrue();
        assertThat(deserialized.range().lowerEndpoint()).isEqualTo(100L);
        assertThat(deserialized.range().hasUpperBound()).isTrue();
        assertThat(deserialized.range().upperEndpoint()).isEqualTo(200L);
    }

    @Test
    void testSerializationWithStartOnly() throws Exception
    {
        TimeRangeFilter original = TimeRangeFilter.startingAt(100L);
        ByteArrayOutputStream baos = serialize(original);
        TimeRangeFilter deserialized = deserialize(baos);
        assertThat(deserialized.range().hasLowerBound()).isTrue();
        assertThat(deserialized.range().lowerEndpoint()).isEqualTo(100L);
        assertThat(deserialized.range().hasUpperBound()).isFalse();
    }

    @Test
    void testSerializationWithEndOnly() throws Exception
    {
        TimeRangeFilter original = TimeRangeFilter.endingAt(200L);
        ByteArrayOutputStream baos = serialize(original);
        TimeRangeFilter deserialized = deserialize(baos);
        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.range().hasLowerBound()).isFalse();
        assertThat(deserialized.range().hasUpperBound()).isTrue();
        assertThat(deserialized.range().upperEndpoint()).isEqualTo(200L);
    }

    private ByteArrayOutputStream serialize(TimeRangeFilter filter) throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(filter);
        oos.close();
        return baos;
    }

    private TimeRangeFilter deserialize(ByteArrayOutputStream baos) throws Exception
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        TimeRangeFilter deserialized = (TimeRangeFilter) ois.readObject();
        ois.close();
        return deserialized;
    }
}
