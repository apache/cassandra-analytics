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

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.CodecRegistry;
import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    public Object deserialize(ByteBuffer buffer)
    {
        return deserialize(buffer, false);
    }

    @Override
    public Object deserialize(ByteBuffer buffer, boolean isFrozen)
    {
        Object value = serializer().deserialize(buffer);
        return value != null ? toSparkSqlType(value) : null;
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

    @Override
    public org.apache.spark.sql.types.DataType sparkSqlType()
    {
        return sparkSqlType(BigNumberConfig.DEFAULT);
    }

    @Override
    public org.apache.spark.sql.types.DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int position)
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    public Object sparkSqlRowValue(Row row, int position)
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

    @Override
    public Object toTestRowType(Object value)
    {
        return value;
    }
}
