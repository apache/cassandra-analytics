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
 * Unit tests for {@link SSTableTimeRangeFilter}
 */
class SSTableTimeRangeFilterTest
{
    @Test
    void testCreation()
    {
        SSTableTimeRangeFilter filter = SSTableTimeRangeFilter.create(100L, 200L);
        assertThat(filter.range().hasLowerBound()).isTrue();
        assertThat(filter.range().hasUpperBound()).isTrue();
        assertThat(filter.range().lowerEndpoint()).isEqualTo(100);
        assertThat(filter.range().upperEndpoint()).isEqualTo(200);
    }

    @Test
    void testThrowsExceptionWhenStartGreaterThanEnd()
    {
        assertThatThrownBy(() -> SSTableTimeRangeFilter.create(200L, 100L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid range: [200‥100]");
    }

    @Test
    void testFiltering()
    {
        SSTableTimeRangeFilter sstableRange = SSTableTimeRangeFilter.create(100L, 200L);
        assertThat(sstableRange.overlaps(0L, 50L)).isFalse();
        assertThat(sstableRange.overlaps(0L, 100L)).isTrue();
        assertThat(sstableRange.overlaps(250L, 300L)).isFalse();
        assertThat(sstableRange.overlaps(200L, 300L)).isTrue();
        assertThat(sstableRange.overlaps(120L, 180L)).isTrue();
        assertThat(sstableRange.overlaps(50L, 150L)).isTrue();
        assertThat(sstableRange.overlaps(150L, 250L)).isTrue();
        assertThat(sstableRange.overlaps(50L, 250L)).isTrue();
    }

    @Test
    void testToString()
    {
        SSTableTimeRangeFilter boundedRangeFilter = SSTableTimeRangeFilter.create(100L, 200L);
        assertThat(boundedRangeFilter.toString()).isEqualTo("TimeRangeFilter[100‥200]");

        SSTableTimeRangeFilter maxBoundRange = SSTableTimeRangeFilter.create(0, Long.MAX_VALUE);
        assertThat(maxBoundRange.toString()).isEqualTo("TimeRangeFilter[0‥9223372036854775807]");

        assertThat(SSTableTimeRangeFilter.ALL.toString()).isEqualTo("TimeRangeFilter[0‥0]");
    }

    @Test
    void testEquals()
    {
        SSTableTimeRangeFilter filter1 = SSTableTimeRangeFilter.create(100L, 200L);
        SSTableTimeRangeFilter filter2 = SSTableTimeRangeFilter.create(100L, 200L);
        SSTableTimeRangeFilter filter4 = SSTableTimeRangeFilter.create(100L, 300L);

        assertThat(filter1).isEqualTo(filter2);
        assertThat(filter1).isNotEqualTo(filter4);
    }

    @Test
    void testHashCode()
    {
        SSTableTimeRangeFilter filter1 = SSTableTimeRangeFilter.create(100L, 200L);
        SSTableTimeRangeFilter filter2 = SSTableTimeRangeFilter.create(100L, 200L);
        SSTableTimeRangeFilter filter3 = SSTableTimeRangeFilter.create(100L, 300L);

        assertThat(filter1.hashCode()).isEqualTo(filter2.hashCode());
        assertThat(filter1.hashCode()).isNotEqualTo(filter3.hashCode());
    }

    @Test
    void testStartEndSerialization() throws Exception
    {
        SSTableTimeRangeFilter original = SSTableTimeRangeFilter.create(100L, 200L);
        ByteArrayOutputStream baos = serialize(original);
        SSTableTimeRangeFilter deserialized = deserialize(baos);
        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.range().hasLowerBound()).isTrue();
        assertThat(deserialized.range().lowerEndpoint()).isEqualTo(100L);
        assertThat(deserialized.range().hasUpperBound()).isTrue();
        assertThat(deserialized.range().upperEndpoint()).isEqualTo(200L);
    }

    @Test
    void testEmptyFilter()
    {
        SSTableTimeRangeFilter emptyFilter = SSTableTimeRangeFilter.ALL;

        assertThat(emptyFilter.overlaps(0L, 100L)).isTrue();
        assertThat(emptyFilter.overlaps(100L, 200L)).isTrue();
        assertThat(emptyFilter.overlaps(1000L, 2000L)).isTrue();
        assertThat(emptyFilter.overlaps(0L, 0L)).isTrue();
    }

    private ByteArrayOutputStream serialize(SSTableTimeRangeFilter filter) throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(filter);
        oos.close();
        return baos;
    }

    private SSTableTimeRangeFilter deserialize(ByteArrayOutputStream baos) throws Exception
    {
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        SSTableTimeRangeFilter deserialized = (SSTableTimeRangeFilter) ois.readObject();
        ois.close();
        return deserialized;
    }
}
