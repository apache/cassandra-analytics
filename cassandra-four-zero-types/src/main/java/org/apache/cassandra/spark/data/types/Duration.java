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

import java.nio.ByteBuffer;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.serializers.DurationSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.data.NativeType;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.jetbrains.annotations.NotNull;

public class Duration extends NativeType
{
    public static final Duration INSTANCE = new Duration();

    @Override
    public String name()
    {
        return "duration";
    }

    @Override
    public AbstractType<?> dataType()
    {
        return DurationType.instance;
    }

    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) CalendarIntervalSerializer.INSTANCE;
    }

    @Override
    public DataType driverDataType(boolean isFrozen)
    {
        return DataType.duration();
    }

    @Override
    public boolean supportedAsPrimaryKeyColumn()
    {
        return false;
    }

    @Override
    public boolean supportedAsMapKey()
    {
        return false;
    }

    @Override
    public boolean supportedAsSetElement()
    {
        return false;
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraVersion version, boolean isCollectionElement)
    {
        CalendarInterval cl = (CalendarInterval) value;
        return isCollectionElement ? CalendarIntervalSerializer.toCqlFunctionDuration(cl) : CalendarIntervalSerializer.toCqlDuration(cl);
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, @NotNull Object value)
    {
        org.apache.cassandra.cql3.functions.types.Duration d = null;
        if (value instanceof CalendarInterval)
        {
            d = CalendarIntervalSerializer.toCqlFunctionDuration((CalendarInterval) value);
        }
        else if (value instanceof org.apache.cassandra.cql3.Duration)
        {
            d = CalendarIntervalSerializer.toCqlFunctionDuration((org.apache.cassandra.cql3.Duration) value);
        }
        else
        {
            d = (org.apache.cassandra.cql3.functions.types.Duration) value;
        }
        udtValue.set(position, d, org.apache.cassandra.cql3.functions.types.Duration.class);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return new CalendarInterval(
            RandomUtils.randomPositiveInt(100),
            RandomUtils.randomPositiveInt(100),
            RandomUtils.randomPositiveInt(1000000000));
    }

    /**
     * Serializes Spark {@link CalendarInterval} as CQL {@link org.apache.cassandra.cql3.Duration}.
     */
    public static class CalendarIntervalSerializer extends TypeSerializer<CalendarInterval>
    {
        private static final CalendarIntervalSerializer INSTANCE = new CalendarIntervalSerializer();

        public ByteBuffer serialize(CalendarInterval duration)
        {
            org.apache.cassandra.cql3.Duration d = toCqlDuration(duration);
            return DurationSerializer.instance.serialize(d);
        }

        public <V> CalendarInterval deserialize(V v, ValueAccessor<V> valueAccessor)
        {
            org.apache.cassandra.cql3.Duration d = DurationSerializer.instance.deserialize(v, valueAccessor);
            return fromCqlDuration(d);
        }

        public <V> void validate(V v, ValueAccessor<V> valueAccessor) throws MarshalException
        {
            DurationSerializer.instance.validate(v, valueAccessor);
        }

        public String toString(CalendarInterval duration)
        {
            return duration == null ? "" : duration.toString();
        }

        public Class<CalendarInterval> getType()
        {
            return CalendarInterval.class;
        }

        public static org.apache.cassandra.cql3.Duration toCqlDuration(CalendarInterval cl)
        {
            if (cl == null)
            {
                return null;
            }
            return org.apache.cassandra.cql3.Duration.newInstance(cl.months, cl.days, cl.microseconds * 1000);
        }

        public static org.apache.cassandra.cql3.functions.types.Duration toCqlFunctionDuration(CalendarInterval cl)
        {
            if (cl == null)
            {
                return null;
            }
            return org.apache.cassandra.cql3.functions.types.Duration.newInstance(cl.months, cl.days, cl.microseconds * 1000);
        }

        public static org.apache.cassandra.cql3.functions.types.Duration toCqlFunctionDuration(org.apache.cassandra.cql3.Duration d)
        {
            if (d == null)
            {
                return null;
            }
            return org.apache.cassandra.cql3.functions.types.Duration.newInstance(d.getMonths(), d.getDays(), d.getNanoseconds());
        }

        public static CalendarInterval fromCqlDuration(org.apache.cassandra.cql3.Duration d)
        {
            if (d == null)
            {
                return null;
            }
            return new CalendarInterval(d.getMonths(), d.getDays(), d.getNanoseconds() / 1000);
        }
    }
}
