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

package org.apache.cassandra.spark.data.converter.types;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;

public class Timestamp implements LongTraits
{
    public static final org.apache.cassandra.spark.data.converter.types.Timestamp INSTANCE = new org.apache.cassandra.spark.data.converter.types.Timestamp();

    private Timestamp()
    {

    }

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.TimestampType;
    }

    @Override
    public Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        return ((java.util.Date) value).getTime() * 1000L; // long
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int position)
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
}
