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

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.LocalDate;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.spark.data.NativeType;
import org.apache.cassandra.spark.utils.RandomUtils;

public class Date extends NativeType
{
    public static final Date INSTANCE = new Date();

    @Override
    public String name()
    {
        return "date";
    }

    @Override
    public AbstractType<?> dataType()
    {
        return SimpleDateType.instance;
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return RandomUtils.randomPositiveInt(30_000);
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, Object value)
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
