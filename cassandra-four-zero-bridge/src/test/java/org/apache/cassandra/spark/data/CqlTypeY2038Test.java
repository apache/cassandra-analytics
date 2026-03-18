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

import java.nio.ByteBuffer;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.SchemaBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests verifying Y2038 boundary behavior for {@link CqlType#tombstone} and {@link CqlType#expiring}
 * in Cassandra 4.0.
 */
class CqlTypeY2038Test
{
    private static final int MAX_TTL_SECONDS = 20 * 365 * 24 * 60 * 60;  // 20 years, Cassandra's max allowed TTL
    private static final long NOW_IN_SEC_AT_BOUNDARY = Integer.MAX_VALUE - (long) MAX_TTL_SECONDS;  // last nowInSec where max TTL fits in int
    private static final long EXCEEDS_INT_RANGE = Integer.MAX_VALUE + 1L;
    private static final long TIMESTAMP = 1000L;
    private static final ByteBuffer VALUE = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});

    private static ColumnMetadata regularColumn()
    {
        String createStatement = "CREATE TABLE test_ks.test_table (pk int PRIMARY KEY, v int)";
        TableMetadata metadata = new SchemaBuilder(createStatement,
                                                   "test_ks",
                                                   new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                                        ImmutableMap.of("replication_factor", 1)),
                                                   Partitioner.Murmur3Partitioner).tableMetaData();
        return metadata.getColumn(ByteBuffer.wrap("v".getBytes()));
    }

    @Test
    void testExpiringWithinIntRange()
    {
        ColumnMetadata column = regularColumn();
        BufferCell cell = CqlType.expiring(column, TIMESTAMP, MAX_TTL_SECONDS, NOW_IN_SEC_AT_BOUNDARY, VALUE, null);
        assertThat(cell).isNotNull();
        assertThat(cell.timestamp()).isEqualTo(TIMESTAMP);
        assertThat(cell.ttl()).isEqualTo(MAX_TTL_SECONDS);
    }

    @Test
    void testExpiringExceedsIntRangeThrows()
    {
        ColumnMetadata column = regularColumn();
        assertThatThrownBy(() -> CqlType.expiring(column, TIMESTAMP, MAX_TTL_SECONDS, EXCEEDS_INT_RANGE, VALUE, null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testTombstoneWithinIntRange()
    {
        ColumnMetadata column = regularColumn();
        BufferCell cell = CqlType.tombstone(column, TIMESTAMP, NOW_IN_SEC_AT_BOUNDARY, null);
        assertThat(cell).isNotNull();
        assertThat(cell.timestamp()).isEqualTo(TIMESTAMP);
    }

    @Test
    void testTombstoneExceedsIntRangeThrows()
    {
        ColumnMetadata column = regularColumn();
        assertThatThrownBy(() -> CqlType.tombstone(column, TIMESTAMP, EXCEEDS_INT_RANGE, null))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
