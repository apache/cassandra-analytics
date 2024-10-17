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
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cdc.api.Row;
import org.apache.cassandra.cql3.functions.types.CodecRegistry;
import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.TypeSerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.data.CqlField.NO_TTL;

public abstract class CqlType implements CqlField.CqlType
{
    public static final CodecRegistry CODEC_REGISTRY = new CodecRegistry();

    @Override
    public CassandraVersion version()
    {
        return CassandraVersion.FOURZERO;
    }

    public abstract AbstractType<?> dataType();

    public abstract AbstractType<?> dataType(boolean isMultiCell);

    @Override
    public Object deserializeToJavaType(ByteBuffer buffer, boolean isFrozen)
    {
        return serializer().deserialize(buffer);
    }

    public abstract <T> TypeSerializer<T> serializer();

    @Override
    public ByteBuffer serialize(Object value)
    {
        return serializer().serialize(value);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        throw CqlField.notImplemented(this);
    }

    public DataType driverDataType()
    {
        return driverDataType(false);
    }

    public DataType driverDataType(boolean isFrozen)
    {
        throw CqlField.notImplemented(this);
    }

    /**
     * Set inner value for UDTs or Tuples
     * @param udtValue udtValue to update
     * @param position position in the vdtValue to set
     * @param value value to set; the value is guaranteed to not be null
     */
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, @NotNull Object value)
    {
        throw CqlField.notImplemented(this);
    }

    /**
     * Set nullable inner value at the position for UDTs or Tuples
     * @param udtValue udtValue to update
     * @param position position in the vdtValue to set
     * @param value nullable value to set
     */
    public final void setInnerValue(SettableByIndexData<?> udtValue, int position, @Nullable Object value)
    {
        if (value == null)
        {
            udtValue.setToNull(position);
        }
        else
        {
            setInnerValueInternal(udtValue, position, value);
        }
    }

    @Override
    public String toString()
    {
        return cqlName();
    }

    @Override
    public int cardinality(int orElse)
    {
        return orElse;
    }

    @VisibleForTesting
    protected static boolean isRowDeletion(CqlTable table, Row row)
    {
        return row.isDeleted();
    }

    @VisibleForTesting
    public void addCell(org.apache.cassandra.db.rows.Row.Builder rowBuilder,
                        ColumnMetadata cd,
                        long timestamp,
                        int ttl,
                        int now,
                        Object value)
    {
        addCell(rowBuilder, cd, timestamp, ttl, now, value, null);
    }

    @VisibleForTesting
    public void addCell(org.apache.cassandra.db.rows.Row.Builder rowBuilder,
                        ColumnMetadata cd,
                        long timestamp,
                        int ttl,
                        int now,
                        Object value,
                        CellPath cellPath)
    {
        if (ttl != NO_TTL)
        {
            rowBuilder.addCell(BufferCell.expiring(cd, timestamp, ttl, now, serialize(value), cellPath));
        }
        else
        {
            rowBuilder.addCell(BufferCell.live(cd, timestamp, serialize(value), cellPath));
        }
    }

    /**
     * Tombstone a simple cell, i.e. it does not work on complex types such as non-frozen collection and UDT
     */
    @VisibleForTesting
    public void addTombstone(org.apache.cassandra.db.rows.Row.Builder rowBuilder,
                             ColumnMetadata cd,
                             long timestamp)
    {
        Preconditions.checkArgument(!cd.isComplex(), "The method only works with non-complex columns");
        addTombstone(rowBuilder, cd, timestamp, null);
    }

    /**
     * Tombstone an element in multi-cells data types such as non-frozen collection and UDT.
     *
     * @param cellPath denotes the element to be tombstoned.
     */
    @VisibleForTesting
    public void addTombstone(org.apache.cassandra.db.rows.Row.Builder rowBuilder,
                             ColumnMetadata cd,
                             long timestamp,
                             CellPath cellPath)
    {
        Preconditions.checkArgument(!(cd.type instanceof ListType), "The method does not support tombstone elements from a List type");
        rowBuilder.addCell(BufferCell.tombstone(cd, timestamp, (int) TimeUnit.MICROSECONDS.toSeconds(timestamp), cellPath));
    }

    /**
     * Tombstone the entire complex cell, i.e. non-frozen collection and UDT
     */
    @VisibleForTesting
    public void addComplexTombstone(org.apache.cassandra.db.rows.Row.Builder rowBuilder,
                                    ColumnMetadata cd,
                                    long deletionTime)
    {
        Preconditions.checkArgument(cd.isComplex(), "The method only works with complex columns");
        rowBuilder.addComplexDeletion(cd, new DeletionTime(deletionTime, (int) TimeUnit.MICROSECONDS.toSeconds(deletionTime)));
    }
}
