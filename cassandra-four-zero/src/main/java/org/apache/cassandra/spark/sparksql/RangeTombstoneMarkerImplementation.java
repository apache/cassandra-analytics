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

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class RangeTombstoneMarkerImplementation implements RangeTombstoneMarker
{
    private final org.apache.cassandra.db.rows.RangeTombstoneMarker marker;

    public RangeTombstoneMarkerImplementation(@NotNull org.apache.cassandra.db.rows.RangeTombstoneMarker marker)
    {
        this.marker = marker;
    }

    @Override
    public boolean isBoundary()
    {
        return marker.isBoundary();
    }

    @Override
    public boolean isOpen(boolean value)
    {
        return marker.isOpen(value);
    }

    @Override
    public boolean isClose(boolean value)
    {
        return marker.isClose(value);
    }

    @Override
    public long openDeletionTime(boolean value)
    {
        return marker.openDeletionTime(value).markedForDeleteAt();
    }

    @Override
    public long closeDeletionTime(boolean value)
    {
        return marker.closeDeletionTime(value).markedForDeleteAt();
    }

    @Override
    @Nullable
    public Object[] computeRange(@Nullable Object[] range, @NotNull List<InternalRow> list, @NotNull CqlTable table)
    {
        if (marker.isBoundary())
        {
            Preconditions.checkState(range != null);
            ClusteringBound<?> close = marker.closeBound(false);
            range[END_FIELD_POSITION] = buildClusteringKey(table, close.clustering());
            range[END_INCLUSIVE_FIELD_POSITION] = close.isInclusive();
            list.add(new GenericInternalRow(range));
            ClusteringBound<?> open = marker.openBound(false);
            range = new Object[TOTAL_FIELDS];
            range[START_FIELD_POSITION] = buildClusteringKey(table, open.clustering());
            range[START_INCLUSIVE_FIELD_POSITION] = open.isInclusive();
        }
        else if (marker.isOpen(false))  // Open bound
        {
            Preconditions.checkState(range == null);
            range = new Object[TOTAL_FIELDS];
            ClusteringBound<?> open = marker.openBound(false);
            range[START_FIELD_POSITION] = buildClusteringKey(table, open.clustering());
            range[START_INCLUSIVE_FIELD_POSITION] = open.isInclusive();
        }
        else  // Close bound
        {
            Preconditions.checkState(range != null);
            ClusteringBound<?> close = marker.closeBound(false);
            range[END_FIELD_POSITION] = buildClusteringKey(table, close.clustering());
            range[END_INCLUSIVE_FIELD_POSITION] = close.isInclusive();
            list.add(new GenericInternalRow(range));
            range = null;
        }
        return range;
    }

    @NotNull
    private static GenericInternalRow buildClusteringKey(@NotNull CqlTable table,
                                                         @NotNull ClusteringPrefix<?> clustering)
    {
        int index = 0;
        Object[] ckFields = new Object[table.numClusteringKeys()];
        for (CqlField field : table.clusteringKeys())
        {
            if (index < clustering.size())
            {
                ByteBuffer buffer = clustering.bufferAt(index);
                ckFields[index] = field.deserialize(buffer);
                index++;
            }
            else
            {
                // A valid range bound does not non-null values following a null value
                break;
            }
        }
        return new GenericInternalRow(ckFields);
    }
}
