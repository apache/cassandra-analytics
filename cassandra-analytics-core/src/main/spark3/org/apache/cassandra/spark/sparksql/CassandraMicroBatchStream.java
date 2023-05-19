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

package org.apache.cassandra.spark.sparksql;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.cdc.CdcRowIterator;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.filters.CdcOffset;
import org.apache.cassandra.spark.sparksql.filters.CdcOffsetFilter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jetbrains.annotations.NotNull;

class CassandraMicroBatchStream implements MicroBatchStream, Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMicroBatchStream.class);
    private static final int DEFAULT_MIN_MUTATION_AGE_SECS = 0;
    private static final int DEFAULT_MAX_MUTATION_AGE_SECS = 300;

    private final DataLayer dataLayer;
    private final StructType requiredSchema;
    private final long minAgeMicros;
    private final long maxAgeMicros;
    private final CdcOffset initial;
    private CdcOffset start;
    private CdcOffset end;

    CassandraMicroBatchStream(DataLayer dataLayer,
                              StructType requiredSchema,
                              CaseInsensitiveStringMap options)
    {
        this.dataLayer = dataLayer;
        this.requiredSchema = requiredSchema;
        this.minAgeMicros = TimeUnit.SECONDS.toMicros(options.getLong("minMutationAgeSeconds", DEFAULT_MIN_MUTATION_AGE_SECS));
        this.maxAgeMicros = TimeUnit.SECONDS.toMicros(options.getLong("maxMutationAgeSeconds", DEFAULT_MAX_MUTATION_AGE_SECS));
        long nowMicros = nowMicros();
        this.initial = start(nowMicros);
        this.start = initial;
        this.end = end(nowMicros);
    }

    private long nowMicros()
    {
        return TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
    }

    @NotNull
    private CdcOffset start(long nowMicros)
    {
        return new CdcOffset(nowMicros - maxAgeMicros);
    }

    @NotNull
    private CdcOffset end(long nowMicros)
    {
        return new CdcOffset(nowMicros - minAgeMicros);
    }

    public Offset initialOffset()
    {
        return initial;
    }

    public Offset latestOffset()
    {
        return end(nowMicros());
    }

    public Offset deserializeOffset(String json)
    {
        return CdcOffset.fromJson(json);
    }

    public void commit(Offset end)
    {
        LOGGER.info("Commit CassandraMicroBatchStream end end='{}'", end);
    }

    public void stop()
    {
        LOGGER.info("Stopping CassandraMicroBatchStream start='{}' end='{}'", start, end);
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end)
    {
        this.start = (CdcOffset) start;
        this.end = (CdcOffset) end;
        int numPartitions = dataLayer.partitionCount();
        LOGGER.info("Planning CDC input partitions numPartitions={} start='{}' end='{}'", numPartitions, start, end);
        return IntStream.range(0, numPartitions)
                        .mapToObj(CassandraInputPartition::new)
                        .toArray(InputPartition[]::new);
    }

    public PartitionReaderFactory createReaderFactory()
    {
        return this::createCdcRowIteratorForPartition;
    }

    private PartitionReader<InternalRow> createCdcRowIteratorForPartition(InputPartition partition)
    {
        Preconditions.checkNotNull(partition, "Null InputPartition");
        if (partition instanceof CassandraInputPartition)
        {
            CassandraInputPartition cassandraInputPartition = (CassandraInputPartition) partition;
            Preconditions.checkNotNull(start, "Start offset was not set");
            LOGGER.info("Opening CdcRowIterator start='{}' end='{}' partitionId={}",
                        start.getTimestampMicros(), end.getTimestampMicros(), cassandraInputPartition.getPartitionId());
            return new CdcRowIterator(cassandraInputPartition.getPartitionId(),
                                      dataLayer,
                                      requiredSchema,
                                      CdcOffsetFilter.of(start, dataLayer.cdcWatermarkWindow()));
        }
        throw new UnsupportedOperationException("Unexpected InputPartition type: " + (partition.getClass().getName()));
    }
}
