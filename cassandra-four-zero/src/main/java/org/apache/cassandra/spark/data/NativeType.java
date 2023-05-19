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

import java.util.Collections;
import java.util.Set;

import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

public abstract class NativeType extends CqlType implements CqlField.NativeType
{
    private final int hashCode;

    protected NativeType()
    {
        hashCode = name().hashCode();
    }

    public CqlField.CqlType.InternalType internalType()
    {
        return CqlField.CqlType.InternalType.NativeCql;
    }

    @Override
    public boolean isSupported()
    {
        return true;
    }

    @Override
    public AbstractType<?> dataType()
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    public Object toSparkSqlType(Object value)
    {
        return toSparkSqlType(value, false);
    }

    @Override
    public Object toSparkSqlType(Object value, boolean isFrozen)
    {
        // All other non-overridden data types work as ordinary Java data types
        return value;
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraVersion version)
    {
        return value;
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int position)
    {
        // We need to convert native types to TestRow types
        return row.isNullAt(position) ? null : toTestRowType(nativeSparkSqlRowValue(row, position));
    }

    protected Object nativeSparkSqlRowValue(GenericInternalRow row, int position)
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    public Object sparkSqlRowValue(Row row, int position)
    {
        // We need to convert native types to TestRow types
        return row.isNullAt(position) ? null : toTestRowType(nativeSparkSqlRowValue(row, position));
    }

    protected Object nativeSparkSqlRowValue(Row row, int position)
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return dataType();
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    @Override
    public boolean equals(Object other)
    {
        return other != null && (this == other || this.getClass() == other.getClass());
    }

    @Override
    public boolean equals(Object first, Object second)
    {
        if (first == second)
        {
            return true;
        }
        else if (first == null || second == null)
        {
            return false;
        }
        else
        {
            return equalsTo(first, second);
        }
    }

    protected boolean equalsTo(Object first, Object second)
    {
        return compare(first, second) == 0;
    }

    @Override
    public int compare(Object first, Object second)
    {
        if (first == null || second == null)
        {
            return first == second ? 0 : (first == null ? -1 : 1);
        }
        return compareTo(first, second);
    }

    protected int compareTo(Object first, Object second)
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) dataType().getSerializer();
    }

    @Override
    public String cqlName()
    {
        return name().toLowerCase();
    }

    @Override
    public void write(Output output)
    {
        CqlField.CqlType.write(this, output);
        output.writeString(name());
    }

    public Set<CqlField.CqlUdt> udts()
    {
        return Collections.emptySet();
    }
}
