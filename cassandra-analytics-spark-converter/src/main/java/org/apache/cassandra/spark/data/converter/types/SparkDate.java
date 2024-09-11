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
import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;

public class SparkDate implements SparkType
{
    public static final SparkDate INSTANCE = new SparkDate();

    private SparkDate()
    {

    }

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.DateType;
    }

    @Override
    public Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        // SparkSQL date type is an int incrementing from day 0 on 1970-01-01
        // Cassandra stores date as "days since 1970-01-01 plus Integer.MIN_VALUE"
        final int days = (Integer) value;
        return days - Integer.MIN_VALUE;
    }

    @Override
    public Object nativeSparkSqlRowValue(GenericInternalRow row, int position)
    {
        return row.getInt(position);
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int position)
    {
        return row.getDate(position);
    }

    @Override
    public int compareTo(Object first, Object second)
    {
        return CqlField.INTEGER_COMPARATOR.compare((Integer) first, (Integer) second);
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof java.sql.Date)
        {
            // Round up to convert date back to days since epoch
            return (int) ((java.sql.Date) value).toLocalDate().toEpochDay();
        }
        else if (value instanceof Integer)
        {
            return ((Integer) value) - Integer.MIN_VALUE;
        }
        return value;
    }
}
