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

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.NativeType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class Timestamp extends NativeType
{
    public static final Timestamp INSTANCE = new Timestamp();

    @Override
    public String name()
    {
        return "timestamp";
    }

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.TimestampType;
    }

    @Override
    public AbstractType<?> dataType()
    {
        return TimestampType.instance;
    }

    @Override
    public Object toSparkSqlType(Object value, boolean isFrozen)
    {
        return ((java.util.Date) value).getTime() * 1000L;  // long
    }

    @Override
    protected int compareTo(Object first, Object second)
    {
        return CqlField.LONG_COMPARATOR.compare((Long) first, (Long) second);
    }

    @Override
    protected Object nativeSparkSqlRowValue(GenericInternalRow row, int position)
    {
        return row.getLong(position);
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int position)
    {
        return new java.util.Date(row.getTimestamp(position).getTime());
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof java.util.Date)
        {
            return value;
        }
        return new java.util.Date((long) value / 1000L);
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setTimestamp(position, (java.util.Date) value);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return new java.util.Date();
    }

    @Override
    public org.apache.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.cql3.functions.types.DataType.timestamp();
    }
}
