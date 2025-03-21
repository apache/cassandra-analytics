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

import java.math.BigInteger;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.spark.utils.RandomUtils;

public class VarInt extends Decimal
{
    public static final VarInt INSTANCE = new VarInt();

    @Override
    public String name()
    {
        return "varint";
    }

    @Override
    public AbstractType<?> dataType()
    {
        return IntegerType.instance;
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return new BigInteger(BigNumberConfig.DEFAULT.bigIntegerPrecision(), RandomUtils.RANDOM);
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setVarint(position, (BigInteger) value);
    }

    @Override
    public org.apache.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.cql3.functions.types.DataType.varint();
    }
}
