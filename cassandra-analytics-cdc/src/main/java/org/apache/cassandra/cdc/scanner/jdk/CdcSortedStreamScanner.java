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

package org.apache.cassandra.cdc.scanner.jdk;

import java.util.Collection;

import org.apache.cassandra.cdc.api.CassandraSource;
import org.apache.cassandra.cdc.msg.AbstractCdcEvent;
import org.apache.cassandra.cdc.msg.jdk.CdcEvent;
import org.apache.cassandra.cdc.msg.jdk.RangeTombstone;
import org.apache.cassandra.cdc.msg.jdk.Value;
import org.apache.cassandra.cdc.scanner.AbstractCdcSortedStreamScanner;
import org.apache.cassandra.cdc.state.CdcState;
import org.apache.cassandra.db.commitlog.PartitionUpdateWrapper;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.jetbrains.annotations.NotNull;

public class CdcSortedStreamScanner extends AbstractCdcSortedStreamScanner<Value, RangeTombstone, CdcEvent>
{
    private final CassandraSource cassandraSource;
    private final Double samplingRate;

    CdcSortedStreamScanner(@NotNull Collection<PartitionUpdateWrapper> updates,
                           @NotNull CdcState endState,
                           CassandraSource cassandraSource,
                           Double samplingRate)
    {
        super(updates, endState);
        this.cassandraSource = cassandraSource;
        this.samplingRate = samplingRate;
    }

    @Override
    public Double samplingRate()
    {
        return samplingRate;
    }

    public CdcEvent buildRowDelete(Row row, UnfilteredRowIterator partition, String trackingId)
    {
        return CdcEvent.Builder.of(AbstractCdcEvent.Kind.ROW_DELETE, partition, trackingId, cassandraSource)
                               .withRow(row)
                               .build();
    }

    @Override
    public CdcEvent buildUpdate(Row row, UnfilteredRowIterator partition, boolean isStaticOnly, String trackingId)
    {
        CdcEvent.Builder builder = CdcEvent.Builder.of(AbstractCdcEvent.Kind.UPDATE, partition, trackingId, cassandraSource);
        if (!isStaticOnly)
        {
            builder.withRow(row);
        }
        return builder.build();
    }

    @Override
    public CdcEvent buildInsert(Row row, UnfilteredRowIterator partition, boolean isStaticOnly, String trackingId)
    {
        CdcEvent.Builder builder = CdcEvent.Builder.of(AbstractCdcEvent.Kind.INSERT, partition, trackingId, cassandraSource);
        if (!isStaticOnly)
        {
            builder.withRow(row);
        }
        return builder.build();
    }

    @Override
    public CdcEvent makePartitionTombstone(UnfilteredRowIterator partition, String trackingId)
    {
        return CdcEvent.Builder.of(AbstractCdcEvent.Kind.PARTITION_DELETE, partition, trackingId, cassandraSource)
                               .build();
    }

    @Override
    public void handleRangeTombstone(RangeTombstoneMarker marker, UnfilteredRowIterator partition, String trackingId)
    {
        if (rangeDeletionBuilder == null)
        {
            rangeDeletionBuilder = CdcEvent.Builder.of(AbstractCdcEvent.Kind.RANGE_DELETE, partition, trackingId, cassandraSource);
        }
        rangeDeletionBuilder.addRangeTombstoneMarker(marker);
    }
}
