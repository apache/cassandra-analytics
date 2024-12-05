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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cdc.api.CassandraSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.utils.ByteBufferUtils.toHexString;

/**
 * Cassandra version independent abstraction of CdcEvent
 */
public class CdcEvent
{
    public enum Kind
    {
        INSERT,
        UPDATE,
        DELETE,
        PARTITION_DELETE,
        ROW_DELETE,
        RANGE_DELETE,
        COMPLEX_ELEMENT_DELETE,
    }

    public static class TimeToLive
    {
        public final int ttlInSec;
        public final int expirationTimeInSec;

        public TimeToLive(int ttlInSec, int expirationTimeInSec)
        {
            this.ttlInSec = ttlInSec;
            this.expirationTimeInSec = expirationTimeInSec;
        }
    }

    @Nullable
    final List<Value> partitionKeys;
    @Nullable
    final List<Value> clusteringKeys;
    @Nullable
    final List<Value> staticColumns;
    @Nullable
    final List<Value> valueColumns;

    // The max timestamp of the cells in the event
    protected final long maxTimestampMicros;
    // Records the ttl info of the event
    final TimeToLive timeToLive;
    // Records the tombstoned elements/cells in a complex data
    final Map<String, List<ByteBuffer>> tombstonedCellsInComplex;
    // Records the range tombstone markers with in the same partition
    final List<RangeTombstone> rangeTombstoneList;
    public final boolean track;
    public final String trackingId;

    public final String keyspace;
    public final String table;
    public final Kind kind;
    protected final CassandraSource cassandraSource;

    // CHECKSTYLE IGNORE: Constructor with many parameters
    protected CdcEvent(CdcEventBuilder builder)
    {
        this.partitionKeys = builder.partitionKeys;
        this.clusteringKeys = builder.clusteringKeys;
        this.staticColumns = builder.staticColumns;
        this.valueColumns = builder.valueColumns;
        this.maxTimestampMicros = builder.maxTimestampMicros;
        this.timeToLive = builder.timeToLive;
        this.tombstonedCellsInComplex = builder.tombstonedCellsInComplex;
        this.rangeTombstoneList = builder.rangeTombstoneList;
        this.track = builder.track;
        this.trackingId = builder.trackingId;
        this.keyspace = builder.keyspace;
        this.table = builder.table;
        this.kind = builder.kind;
        this.cassandraSource = builder.cassandraSource;
    }

    /**
     * @return the kind of the cdc event.
     */
    public Kind getKind()
    {
        return kind;
    }

    /**
     * @return the timestamp of the cdc event in {@link TimeUnit}
     */
    public long getTimestamp(TimeUnit timeUnit)
    {
        return timeUnit.convert(maxTimestampMicros, TimeUnit.MICROSECONDS);
    }

    /**
     * @return the partition keys. The returned list must not be null and empty.
     */
    @NotNull
    public List<Value> getPartitionKeys()
    {
        return partitionKeys;
    }

    /**
     * @return the clustering keys. The returned list could be null if the mutation carries no clustering keys.
     */
    @Nullable
    public List<Value> getClusteringKeys()
    {
        return clusteringKeys;
    }

    private List<Value> getPrimaryKeyColumns()
    {
        if (clusteringKeys == null)
        {
            return getPartitionKeys();
        }
        else
        {
            List<Value> primaryKeys = new ArrayList<>(partitionKeys.size() + clusteringKeys.size());
            primaryKeys.addAll(partitionKeys);
            primaryKeys.addAll(clusteringKeys);
            return primaryKeys;
        }
    }

    /**
     * @return the static columns. The returned list could be null if the mutation carries no static columns.
     */
    @Nullable
    public List<Value> getStaticColumns()
    {
        return staticColumns;
    }

    /**
     * @return the value columns. The returned list could be null if the mutation carries no value columns.
     */
    @Nullable
    public List<Value> getValueColumns()
    {
        return valueColumns;
    }

    /**
     * The map returned contains the list of deleted keys (i.e. cellpath in Cassandra's terminology) of each affected
     * complext column. A complex column could be an unfrozen map, set and udt in Cassandra.
     *
     * @return the tombstoned cells in the complex data columns. The returned map could be null if the mutation does not
     * delete elements from complex.
     */
    @Nullable
    public Map<String, List<ByteBuffer>> getTombstonedCellsInComplex()
    {
        return tombstonedCellsInComplex;
    }

    /**
     * @return the range tombstone list. The returned list could be null if the mutation is not a range deletin.
     */
    @Nullable
    public List<RangeTombstone> getRangeTombstoneList()
    {
        return rangeTombstoneList;
    }

    /**
     * @return the time to live. The returned value could be null if the mutation carries no such value.
     */
    @Nullable
    public TimeToLive getTtl()
    {
        return timeToLive;
    }

    // convert primary keys into a hex string - useful in the tests to match expected with actual rows.
    public String getHexKey()
    {
        StringBuilder str = new StringBuilder();
        for (Value column : getPrimaryKeyColumns())
        {
            str.append(toHexString(column.getValue())).append(':');
        }
        return str.toString();
    }
}
