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

package org.apache.cassandra.cdc.msg;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cdc.api.CassandraSource;
import org.jetbrains.annotations.Nullable;

public class CdcEventBuilder
{
    @Nullable
    protected List<Value> partitionKeys = null;
    @Nullable
    protected List<Value> clusteringKeys = null;
    @Nullable
    protected List<Value> staticColumns = null;
    @Nullable
    protected List<Value> valueColumns = null;

    // The max timestamp of the cells in the event
    protected long maxTimestampMicros = Long.MIN_VALUE;
    // Records the ttl info of the event
    @Nullable
    protected CdcEvent.TimeToLive timeToLive = null;
    // Records the tombstoned elements/cells in a complex data
    @Nullable
    protected Map<String, List<ByteBuffer>> tombstonedCellsInComplex = null;
    // Records the range tombstone markers with in the same partition
    @Nullable
    protected List<RangeTombstone> rangeTombstoneList = null;
    @Nullable
    protected RangeTombstoneBuilder<?> rangeTombstoneBuilder = null;
    public boolean track;
    @Nullable
    public String trackingId;
    public String keyspace;
    public String table;
    public CdcEvent.Kind kind;
    protected CassandraSource cassandraSource;

    protected CdcEventBuilder(CdcEvent.Kind kind, String keyspace, String table, String trackingId, CassandraSource cassandraSource)
    {
        this.kind = kind;
        this.keyspace = keyspace;
        this.table = table;
        this.trackingId = trackingId;
        this.track = trackingId != null;
        this.cassandraSource = cassandraSource;
    }

    public void setPartitionKeys(@Nullable List<Value> partitionKeys)
    {
        this.partitionKeys = partitionKeys;
    }

    public void setClusteringKeys(@Nullable List<Value> clusteringKeys)
    {
        this.clusteringKeys = clusteringKeys;
    }

    public void setStaticColumns(@Nullable List<Value> staticColumns)
    {
        this.staticColumns = staticColumns;
    }

    public void setValueColumns(@Nullable List<Value> valueColumns)
    {
        this.valueColumns = valueColumns;
    }

    public void setMaxTimestampMicros(long maxTimestampMicros)
    {
        this.maxTimestampMicros = maxTimestampMicros;
    }

    public void setTTL(int ttlInSec, int expirationTimeInSec)
    {
        // Skip updating TTL if it already has been set.
        // For the same row, the upsert query can only set one TTL value.
        if (timeToLive != null)
        {
            return;
        }

        setTimeToLive(new CdcEvent.TimeToLive(ttlInSec, expirationTimeInSec));
    }

    public void setTimeToLive(CdcEvent.TimeToLive timeToLive)
    {
        this.timeToLive = timeToLive;
    }

    public void setTombstonedCellsInComplex(Map<String, List<ByteBuffer>> tombstonedCellsInComplex)
    {
        this.tombstonedCellsInComplex = tombstonedCellsInComplex;
    }

    public void setRangeTombstoneList(List<RangeTombstone> rangeTombstoneList)
    {
        this.rangeTombstoneList = rangeTombstoneList;
    }

    public void setRangeTombstoneBuilder(RangeTombstoneBuilder<?> rangeTombstoneBuilder)
    {
        this.rangeTombstoneBuilder = rangeTombstoneBuilder;
    }

    public void setTrack(boolean track)
    {
        this.track = track;
    }

    public void setTrackingId(String trackingId)
    {
        this.trackingId = trackingId;
        this.track = trackingId != null;
    }

    public void setKeyspace(String keyspace)
    {
        this.keyspace = keyspace;
    }

    public void setTable(String table)
    {
        this.table = table;
    }

    public void setKind(CdcEvent.Kind kind)
    {
        this.kind = kind;
    }

    public void setCassandraSource(CassandraSource cassandraSource)
    {
        this.cassandraSource = cassandraSource;
    }

    // adds the serialized cellpath to the tombstone
    public void addCellTombstoneInComplex(String columnName, ByteBuffer key)
    {
        if (tombstonedCellsInComplex == null)
        {
            tombstonedCellsInComplex = new HashMap<>();
        }
        List<ByteBuffer> tombstones = tombstonedCellsInComplex.computeIfAbsent(columnName, k -> new ArrayList<>());
        tombstones.add(key);
    }

    // Update the maxTimestamp if the input `timestamp` is larger.
    protected void updateMaxTimestamp(long timestamp)
    {
        maxTimestampMicros = Math.max(maxTimestampMicros, timestamp);
    }

    public void validateRangeTombstoneMarkers()
    {
        if (rangeTombstoneList == null)
        {
            return;
        }

        Preconditions.checkState(!rangeTombstoneBuilder.hasIncompleteRange(),
                                 "The last range tombstone is not closed");
    }

    public CdcEvent build()
    {
        validateRangeTombstoneMarkers();
        return new CdcEvent(this);
    }
}
