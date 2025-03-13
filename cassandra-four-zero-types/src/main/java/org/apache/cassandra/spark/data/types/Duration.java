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
import org.apache.cassandra.bridge.type.CqlDuration;
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
        CqlDuration cl = CqlDuration.from(value);
        return isCollectionElement ? CalendarIntervalSerializer.toCql3FunctionDuration(cl) : CalendarIntervalSerializer.toCql3Duration(cl);
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, @NotNull Object value)
    {
        org.apache.cassandra.cql3.functions.types.Duration duration = null;
        if (value instanceof CqlDuration)
        {
            duration = CalendarIntervalSerializer.toCql3FunctionDuration((CqlDuration) value);
        }
        else if (value instanceof org.apache.cassandra.cql3.Duration)
        {
            duration = CalendarIntervalSerializer.toCql3FunctionDuration((org.apache.cassandra.cql3.Duration) value);
        }
        else
        {
            duration = (org.apache.cassandra.cql3.functions.types.Duration) value;
        }
        udtValue.set(position, duration, org.apache.cassandra.cql3.functions.types.Duration.class);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        // returns CalendarInterval
        return new CqlDuration(
            RandomUtils.randomPositiveInt(100),
            RandomUtils.randomPositiveInt(100),
            // CalendarInterval does not support nanosecond precision
            RandomUtils.randomPositiveInt(1000000) * 1000L).asCalendarInterval();
    }

    /**
     * Serializes Spark {@code CalendarInterval} type as CQL {@link org.apache.cassandra.cql3.Duration}.
     * Implementation note: {@code TypeSerializer<Object>} is used to prevent class cast exception from
     * {@code CalendarInterval}, which is not direct dependency of this module.
     */
    public static class CalendarIntervalSerializer extends TypeSerializer<Object>
    {
        private static final CalendarIntervalSerializer INSTANCE = new CalendarIntervalSerializer();

        public ByteBuffer serialize(Object value)
        {
            CqlDuration wrapper = CqlDuration.from(value);
            org.apache.cassandra.cql3.Duration cqlDuration = toCql3Duration(wrapper);
            return DurationSerializer.instance.serialize(cqlDuration);
        }

        public <V> Object deserialize(V v, ValueAccessor<V> valueAccessor)
        {
            org.apache.cassandra.cql3.Duration cqlDuration = DurationSerializer.instance.deserialize(v, valueAccessor);
            return fromCql3Duration(cqlDuration).asCalendarInterval();
        }

        public <V> void validate(V v, ValueAccessor<V> valueAccessor) throws MarshalException
        {
            DurationSerializer.instance.validate(v, valueAccessor);
        }

        public String toString(Object duration)
        {
            return duration == null ? "" : duration.toString();
        }

        public Class<Object> getType()
        {
            return Object.class;
        }

        public static org.apache.cassandra.cql3.Duration toCql3Duration(CqlDuration cl)
        {
            if (cl == null)
            {
                return null;
            }
            return org.apache.cassandra.cql3.Duration.newInstance(cl.getMonths(), cl.getDays(), cl.getNanoseconds());
        }

        public static org.apache.cassandra.cql3.functions.types.Duration toCql3FunctionDuration(CqlDuration cl)
        {
            if (cl == null)
            {
                return null;
            }
            return org.apache.cassandra.cql3.functions.types.Duration.newInstance(cl.getMonths(), cl.getDays(), cl.getNanoseconds());
        }

        public static org.apache.cassandra.cql3.functions.types.Duration toCql3FunctionDuration(org.apache.cassandra.cql3.Duration d)
        {
            if (d == null)
            {
                return null;
            }
            return org.apache.cassandra.cql3.functions.types.Duration.newInstance(d.getMonths(), d.getDays(), d.getNanoseconds());
        }

        public static CqlDuration fromCql3Duration(org.apache.cassandra.cql3.Duration d)
        {
            if (d == null)
            {
                return null;
            }
            return new CqlDuration(d.getMonths(), d.getDays(), d.getNanoseconds());
        }
    }
}
