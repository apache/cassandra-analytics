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
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.LocalDate;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.NativeType;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;

public class Date extends NativeType
{
    public static final Date INSTANCE = new Date();

    @Override
    public String name()
    {
        return "date";
    }

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.DateType;
    }

    @Override
    public Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        // SparkSQL date type is an int incrementing from day 0 on 1970-01-01
        // Cassandra stores date as "days since 1970-01-01 plus Integer.MIN_VALUE"
        int days = (Integer) value;
        return days - Integer.MIN_VALUE;
    }

    @Override
    public AbstractType<?> dataType()
    {
        return SimpleDateType.instance;
    }

    @Override
    protected int compareTo(Object first, Object second)
    {
        return CqlField.INTEGER_COMPARATOR.compare((Integer) first, (Integer) second);
    }

    @Override
    protected Object nativeSparkSqlRowValue(GenericInternalRow row, int position)
    {
        return row.getInt(position);
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int position)
    {
        return row.getDate(position);
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

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return RandomUtils.randomPositiveInt(30_000);
    }

    @Override
    protected void setInnerValue(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setDate(position, (LocalDate) value);
    }

    @Override
    public org.apache.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.cql3.functions.types.DataType.date();
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraVersion version)
    {
        // Cassandra 4.0 no longer allows writing date types as Integers in CqlWriter,
        // so we need to convert to LocalDate before writing in tests
        if (version == CassandraVersion.FOURZERO)
        {
            return LocalDate.fromDaysSinceEpoch(((int) value));
        }
        return value;
    }
}
