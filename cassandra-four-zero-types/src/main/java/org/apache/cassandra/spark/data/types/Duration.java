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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.type.InternalDuration;
import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.data.NativeType;
import org.apache.cassandra.spark.utils.RandomUtils;
import javax.annotation.Nonnull;

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
        return (TypeSerializer<T>) AnalyticsDurationSerializer.INSTANCE;
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
        InternalDuration duration = (InternalDuration) value;
        return isCollectionElement
               ? AnalyticsDurationSerializer.toCql3FunctionDuration(duration)
               : AnalyticsDurationSerializer.toCql3Duration(duration);
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, @Nonnull Object value)
    {
        org.apache.cassandra.cql3.functions.types.Duration duration = null;
        if (value instanceof InternalDuration)
        {
            duration = AnalyticsDurationSerializer.toCql3FunctionDuration((InternalDuration) value);
        }
        else if (value instanceof org.apache.cassandra.cql3.Duration)
        {
            duration = AnalyticsDurationSerializer.toCql3FunctionDuration((org.apache.cassandra.cql3.Duration) value);
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
        return new InternalDuration(RandomUtils.randomPositiveInt(100),
                                    RandomUtils.randomPositiveInt(100),
                                    TimeUnit.MICROSECONDS.toNanos(RandomUtils.randomPositiveInt(1000000)));
    }

    /**
     * Serializes Spark {@code CalendarInterval} type as CQL {@link org.apache.cassandra.cql3.Duration}.
     * Implementation note: {@code TypeSerializer<Object>} is used to prevent class cast exception from
     * {@code CalendarInterval}, which is not direct dependency of this module.
     */
    private static class AnalyticsDurationSerializer extends TypeSerializer<InternalDuration>
    {
        private static final AnalyticsDurationSerializer INSTANCE = new AnalyticsDurationSerializer();

        @Override
        public ByteBuffer serialize(InternalDuration value)
        {
            org.apache.cassandra.cql3.Duration cqlDuration = toCql3Duration(value);
            return org.apache.cassandra.serializers.DurationSerializer.instance.serialize(cqlDuration);
        }

        public <V> InternalDuration deserialize(V v, ValueAccessor<V> valueAccessor)
        {
            org.apache.cassandra.cql3.Duration cqlDuration = org.apache.cassandra.serializers.DurationSerializer.instance.deserialize(v, valueAccessor);
            return fromCql3Duration(cqlDuration);
        }

        public <V> void validate(V v, ValueAccessor<V> valueAccessor) throws MarshalException
        {
            org.apache.cassandra.serializers.DurationSerializer.instance.validate(v, valueAccessor);
        }

        public String toString(InternalDuration duration)
        {
            return duration == null ? "" : duration.toString();
        }

        public Class<InternalDuration> getType()
        {
            return InternalDuration.class;
        }

        public static org.apache.cassandra.cql3.Duration toCql3Duration(InternalDuration duration)
        {
            return nullOrConvert(duration, d -> org.apache.cassandra.cql3.Duration.newInstance(d.months, d.days, d.nanoseconds));
        }

        public static org.apache.cassandra.cql3.functions.types.Duration toCql3FunctionDuration(InternalDuration duration)
        {
            return nullOrConvert(duration, d -> org.apache.cassandra.cql3.functions.types.Duration.newInstance(d.months, d.days, d.nanoseconds));
        }

        public static org.apache.cassandra.cql3.functions.types.Duration toCql3FunctionDuration(org.apache.cassandra.cql3.Duration duration)
        {
            return nullOrConvert(duration, d -> org.apache.cassandra.cql3.functions.types.Duration.newInstance(d.getMonths(), d.getDays(), d.getNanoseconds()));
        }

        public static InternalDuration fromCql3Duration(org.apache.cassandra.cql3.Duration duration)
        {
            return nullOrConvert(duration, d -> new InternalDuration(d.getMonths(), d.getDays(), d.getNanoseconds()));
        }

        private static <I, O> O nullOrConvert(I input, Function<I, O> converter)
        {
            if (input == null)
            {
                return null;
            }

            return converter.apply(input);
        }
    }
}
