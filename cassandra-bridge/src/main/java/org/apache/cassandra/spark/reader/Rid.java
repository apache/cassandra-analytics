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

package org.apache.cassandra.spark.reader;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.spark.sparksql.RangeTombstoneMarker;
import org.apache.cassandra.spark.utils.ByteBufferUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Rid - Row Identifier - contains the partition key, clustering keys and column name that uniquely identifies a row and column of data in Cassandra
 */
public class Rid
{
    private ByteBuffer partitionKey;
    private ByteBuffer columnName;
    private ByteBuffer value;
    private long timestamp;
    private BigInteger token;
    @VisibleForTesting
    boolean isNewPartition = false;
    private boolean isRowDeletion = false;
    private boolean isPartitionDeletion = false;
    private boolean isUpdate = false;

    // Optional field; memorizes tombstoned elements/cells in a complex data; only used in CDC
    @Nullable
    private List<ByteBuffer> tombstonedCellsInComplex = null;

    // Optional field; memorizes the range tombstone markers with in the same partition; only used in CDC
    private List<RangeTombstoneMarker> rangeTombstoneMarkers = null;
    private boolean shouldConsumeRangeTombstoneMarkers = false;

    // Partition Key Value

    public void setPartitionKeyCopy(ByteBuffer partitionKeyBytes, BigInteger token)
    {
        this.partitionKey = partitionKeyBytes;
        this.token = token;
        this.columnName = null;
        this.value = null;
        this.isNewPartition = true;
        this.timestamp = 0L;
    }

    public boolean isNewPartition()
    {
        if (isNewPartition)
        {
            isNewPartition = false;
            return true;
        }
        return false;
    }

    public boolean isPartitionDeletion()
    {
        return isPartitionDeletion;
    }

    public void setPartitionDeletion(boolean isPartitionDeletion)
    {
        this.isPartitionDeletion = isPartitionDeletion;
    }

    public boolean isUpdate()
    {
        return isUpdate;
    }

    public void setIsUpdate(boolean isUpdate)
    {
        this.isUpdate = isUpdate;
    }

    public boolean isRowDeletion()
    {
        return isRowDeletion;
    }

    public void setRowDeletion(boolean isRowDeletion)
    {
        this.isRowDeletion = isRowDeletion;
    }

    public ByteBuffer getPartitionKey()
    {
        return partitionKey;
    }

    public BigInteger getToken()
    {
        return token;
    }

    // Column Name (contains concatenated clustering keys and column name)

    public void setColumnNameCopy(ByteBuffer columnBytes)
    {
        this.columnName = columnBytes;
    }

    public ByteBuffer getColumnName()
    {
        return columnName;
    }

    // Value of Cell

    public ByteBuffer getValue()
    {
        return value;
    }

    public void setValueCopy(ByteBuffer value)
    {
        this.value = value;
    }

    // Timestamp

    public void setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    // CDC: Handle element deletion in complex, adds the serialized cellpath to the tombstone
    public void addCellTombstoneInComplex(ByteBuffer key)
    {
        if (tombstonedCellsInComplex == null)
        {
            tombstonedCellsInComplex = new ArrayList<>();
        }
        tombstonedCellsInComplex.add(key);
    }

    public boolean hasCellTombstoneInComplex()
    {
        return tombstonedCellsInComplex != null && !tombstonedCellsInComplex.isEmpty();
    }

    public List<ByteBuffer> getCellTombstonesInComplex()
    {
        return tombstonedCellsInComplex;
    }

    public void resetCellTombstonesInComplex()
    {
        tombstonedCellsInComplex = null;
    }

    public void addRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        if (rangeTombstoneMarkers == null)
        {
            rangeTombstoneMarkers = new ArrayList<>();
        }

        // Ensure the marker list is valid
        if (rangeTombstoneMarkers.isEmpty())
        {
            Preconditions.checkArgument(!marker.isBoundary() && marker.isOpen(false),
                                        "The first marker should be an open bound");
            rangeTombstoneMarkers.add(marker);
        }
        else
        {
            RangeTombstoneMarker lastMarker = rangeTombstoneMarkers.get(rangeTombstoneMarkers.size() - 1);
            Preconditions.checkArgument((lastMarker.isOpen(false) && marker.isClose(false))
                                     || (lastMarker.isClose(false) && marker.isOpen(false)),
                                        "Current marker should close or open a new range");
            rangeTombstoneMarkers.add(marker);
        }
    }

    public boolean hasRangeTombstoneMarkers()
    {
        return rangeTombstoneMarkers != null && !rangeTombstoneMarkers.isEmpty();
    }

    public void setShouldConsumeRangeTombstoneMarkers(boolean shouldConsumeRangeTombstoneMarkers)
    {
        this.shouldConsumeRangeTombstoneMarkers = shouldConsumeRangeTombstoneMarkers;
    }

    public boolean shouldConsumeRangeTombstoneMarkers()
    {
        return shouldConsumeRangeTombstoneMarkers;
    }

    public List<RangeTombstoneMarker> getRangeTombstoneMarkers()
    {
        return rangeTombstoneMarkers;
    }

    public void resetRangeTombstoneMarkers()
    {
        rangeTombstoneMarkers = null;
        shouldConsumeRangeTombstoneMarkers = false;
    }

    @Override
    public String toString()
    {
        return ByteBufferUtils.toHexString(getPartitionKey()) + ":"
             + ByteBufferUtils.toHexString(getColumnName()) + ":"
             + ByteBufferUtils.toHexString(getValue());
    }
}
