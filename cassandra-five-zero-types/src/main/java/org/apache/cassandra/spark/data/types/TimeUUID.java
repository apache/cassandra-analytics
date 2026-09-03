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
import java.util.Random;
import java.util.function.Function;

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;

public class TimeUUID extends AbstractTimeUUID
{
    public static final TimeUUID INSTANCE = new TimeUUID();

    @Override
    public Object randomValue(int minCollectionSize, Random random)
    {
        // Cassandra's own monotonic time-based generator is not pluggable with an external Random source,
        // so this remains unseeded; see the "explicitly out of scope" note for QT reproducibility.
        return org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID().asUUID();
    }

    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) AnalyticsTimeUUIDSerializer.INSTANCE;
    }

    private static class AnalyticsTimeUUIDSerializer extends TypeSerializer<java.util.UUID>
    {
        private static final AnalyticsTimeUUIDSerializer INSTANCE = new AnalyticsTimeUUIDSerializer();

        @Override
        public ByteBuffer serialize(java.util.UUID value)
        {
            org.apache.cassandra.utils.TimeUUID timeUuid = nullOrConvert(value, org.apache.cassandra.utils.TimeUUID::fromUuid);
            return org.apache.cassandra.utils.TimeUUID.Serializer.instance.serialize(timeUuid);
        }

        public <V> java.util.UUID deserialize(V v, ValueAccessor<V> valueAccessor)
        {
            org.apache.cassandra.utils.TimeUUID timeUuid = org.apache.cassandra.utils.TimeUUID.Serializer.instance.deserialize(v, valueAccessor);
            return nullOrConvert(timeUuid, org.apache.cassandra.utils.TimeUUID::asUUID);
        }

        public <V> void validate(V v, ValueAccessor<V> valueAccessor) throws MarshalException
        {
            org.apache.cassandra.utils.TimeUUID.Serializer.instance.validate(v, valueAccessor);
        }

        public String toString(java.util.UUID uuid)
        {
            return uuid == null ? "" : uuid.toString();
        }

        public Class<java.util.UUID> getType()
        {
            return java.util.UUID.class;
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
