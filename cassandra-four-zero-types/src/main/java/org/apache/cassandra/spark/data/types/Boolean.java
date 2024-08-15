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
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.NativeType;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class Boolean extends NativeType
{
    public static final Boolean INSTANCE = new Boolean();

    @Override
    public String name()
    {
        return "boolean";
    }

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.BooleanType;
    }

    @Override
    public AbstractType<?> dataType()
    {
        return BooleanType.instance;
    }

    @Override
    protected int compareTo(Object first, Object second)
    {
        return CqlField.BOOLEAN_COMPARATOR.compare((java.lang.Boolean) first, (java.lang.Boolean) second);
    }

    @Override
    public int cardinality(int orElse)
    {
        return 2;
    }

    @Override
    protected Object nativeSparkSqlRowValue(GenericInternalRow row, int position)
    {
        return row.getBoolean(position);
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int position)
    {
        return row.getBoolean(position);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return RandomUtils.RANDOM.nextBoolean();
    }

    @Override
    protected void setInnerValue(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setBool(position, (boolean) value);
    }

    @Override
    public org.apache.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.cql3.functions.types.DataType.cboolean();
    }
}
