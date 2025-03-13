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

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.bridge.type.InternalDuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.jetbrains.annotations.NotNull;

public class SparkDuration implements SparkType
{
    public static final SparkDuration INSTANCE = new SparkDuration();
    private static final Comparator<CalendarInterval> CALENDAR_INTERVAL_COMPARATOR =
    Comparator.<CalendarInterval>comparingInt(interval -> interval.months)
              .thenComparingInt(interval -> interval.days)
              .thenComparingLong(interval -> interval.microseconds);

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.CalendarIntervalType;
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int position)
    {
        return row.isNullAt(position) ? null : row.get(position); // should return CalendarInterval type
    }

    @Override
    public Object nativeSparkSqlRowValue(final GenericInternalRow row, final int position)
    {
        return row.isNullAt(position) ? null : row.getInterval(position);
    }

    @Override
    public Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        InternalDuration duration = (InternalDuration) value;
        // Unfortunately, it loses precision when converting to the spark data type.
        return new CalendarInterval(duration.months, duration.days, TimeUnit.NANOSECONDS.toMicros(duration.nanoseconds));
    }

    @Override
    public Object toTestRowType(Object value)
    {
        CalendarInterval ci = (CalendarInterval) value;
        return new InternalDuration(ci.months, ci.days, TimeUnit.MICROSECONDS.toNanos( ci.microseconds));
    }

    @Override
    public int compareTo(Object first, Object second)
    {
        return CALENDAR_INTERVAL_COMPARATOR.compare((CalendarInterval) first, (CalendarInterval) second);
    }
}
