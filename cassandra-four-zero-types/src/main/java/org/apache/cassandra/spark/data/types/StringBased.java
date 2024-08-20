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

package org.apache.cassandra.spark.data.types;

import java.util.Comparator;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.spark.data.NativeType;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

public abstract class StringBased extends NativeType
{
    private static final Comparator<String> STRING_COMPARATOR = String::compareTo;

    @Override
    public Object toSparkSqlType(Object value, boolean isFrozen)
    {
        if (value == null)
        {
            return null;
        }
        return UTF8String.fromString(value.toString());  // UTF8String
    }

    @Override
    protected int compareTo(Object first, Object second)
    {
        return STRING_COMPARATOR.compare(first.toString(), second.toString());
    }

    @Override
    protected boolean equalsTo(Object first, Object second)
    {
        // UUID comparator is particularly slow because of UUID.fromString so compare for equality as strings
        return first.equals(second);
    }

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.StringType;
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof UTF8String)
        {
            return ((UTF8String) value).toString();
        }
        return value;
    }

    @Override
    protected Object nativeSparkSqlRowValue(GenericInternalRow row, int position)
    {
        return row.getString(position);
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int position)
    {
        return row.getString(position);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return RandomUtils.randomAlphanumeric(RandomUtils.randomPositiveInt(32));
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setString(position, (String) value);
    }
}
