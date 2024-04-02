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

import java.util.Comparator;

import org.apache.cassandra.cql3.functions.types.DataType;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UUIDType;

public class UUID extends StringBased
{
    public static final UUID INSTANCE = new UUID();
    private static final Comparator<String> UUID_COMPARATOR = Comparator.comparing(java.util.UUID::fromString);

    @Override
    public String name()
    {
        return "uuid";
    }

    @Override
    public AbstractType<?> dataType()
    {
        return UUIDType.instance;
    }

    @Override
    protected int compareTo(Object first, Object second)
    {
        return UUID_COMPARATOR.compare(first.toString(), second.toString());
    }

    @Override
    public Object toTestRowType(Object value)
    {
        return java.util.UUID.fromString(value.toString());
    }

    @Override
    public DataType driverDataType(boolean isFrozen)
    {
        return DataType.uuid();
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setUUID(position, (java.util.UUID) value);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return java.util.UUID.randomUUID();
    }
}
