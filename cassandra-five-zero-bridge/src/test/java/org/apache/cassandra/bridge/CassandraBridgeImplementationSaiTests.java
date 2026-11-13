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
package org.apache.cassandra.bridge;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.analytics.stats.Stats;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.spark.data.BasicSupplier;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.SSTable;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.reader.EmptyStreamScanner;
import org.apache.cassandra.spark.sparksql.filters.SSTableTimeRangeFilter;
import org.apache.cassandra.spark.sparksql.filters.SparkRangeFilter;
import org.apache.cassandra.spark.utils.Pair;
import org.apache.cassandra.spark.utils.TimeProvider;
import org.apache.cassandra.utils.TokenUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CassandraBridgeImplementationSaiTests
{
    @Test
    void testNonOverlappingSSTableIsPrunedBeforeSaiResourcesAreOpened()
    {
        SSTable sstable = mock(SSTable.class);
        when(sstable.isBigFormat()).thenReturn(true);
        CassandraBridgeImplementation bridge = new CassandraBridgeImplementation()
        {
            @Override
            protected Pair<DecoratedKey, DecoratedKey> getFirstLastKeyFromSSTable(TableMetadata metadata,
                                                                                  SSTable sstable)
            {
                Token first = TokenUtils.bigIntegerToToken(metadata.partitioner, BigInteger.ZERO);
                Token last = TokenUtils.bigIntegerToToken(metadata.partitioner, BigInteger.TEN);
                return Pair.of(new BufferDecoratedKey(first, ByteBuffer.allocate(0)),
                               new BufferDecoratedKey(last, ByteBuffer.allocate(0)));
            }
        };
        CqlTable table = bridge.buildSchema("CREATE TABLE sai_pruning_tbl (pk bigint PRIMARY KEY, value text)",
                                            "sai_pruning_ks");
        SparkRangeFilter workerRange = SparkRangeFilter.create(TokenRange.closed(BigInteger.valueOf(100),
                                                                                 BigInteger.valueOf(200)));

        assertThat(bridge.getCompactionScanner(table,
                                               Partitioner.Murmur3Partitioner,
                                               new BasicSupplier(Collections.singleton(sstable)),
                                               workerRange,
                                               Collections.emptyList(),
                                               SSTableTimeRangeFilter.ALL,
                                               null,
                                               TimeProvider.DEFAULT,
                                               false,
                                               false,
                                               Stats.DoNothingStats.INSTANCE,
                                               Collections.singletonList(null),
                                               Integer.MAX_VALUE,
                                               Executors.newSingleThreadExecutor()))
        .isSameAs(EmptyStreamScanner.INSTANCE);

        verify(sstable, never()).saiComponentNames();
    }
}
