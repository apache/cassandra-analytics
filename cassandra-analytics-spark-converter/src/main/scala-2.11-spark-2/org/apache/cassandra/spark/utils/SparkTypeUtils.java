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

package org.apache.cassandra.spark.utils;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.bridge.type.InternalDuration;
import org.apache.spark.unsafe.types.CalendarInterval;

public final class SparkTypeUtils
{
    private SparkTypeUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static final Comparator<CalendarInterval> CALENDAR_INTERVAL_COMPARATOR =
    Comparator.<CalendarInterval>comparingInt(interval -> interval.months)
              .thenComparingLong(interval -> interval.microseconds);

    public static CalendarInterval convertDuration(InternalDuration duration)
    {
        // Unfortunately, it loses precision when converting to the spark data type.
        long micros = TimeUnit.NANOSECONDS.toMicros(duration.nanoseconds);
        micros += duration.days * CalendarInterval.MICROS_PER_DAY;
        return new CalendarInterval(duration.months, micros);
    }

    public static InternalDuration convertDuration(CalendarInterval interval)
    {
        int days = interval.microseconds / CalendarInterval.MICROS_PER_DAY;
        long microsRemain = interval.microseconds % CalendarInterval.MICROS_PER_DAY;
        return new InternalDuration(interval.months, days, TimeUnit.MICROSECONDS.toNanos(microsRemain));
    }
}
