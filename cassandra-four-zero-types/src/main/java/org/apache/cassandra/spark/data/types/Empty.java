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
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.NativeType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class Empty extends NativeType
{
    public static final Empty INSTANCE = new Empty();

    @Override
    public boolean isSupported()
    {
        return false;
    }

    @Override
    public String name()
    {
        return "empty";
    }

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.NullType;
    }

    @Override
    public AbstractType<?> dataType()
    {
        return EmptyType.instance;
    }

    @Override
    protected int compareTo(Object first, Object second)
    {
        return CqlField.VOID_COMPARATOR_COMPARATOR.compare((Void) first, (Void) second);
    }

    @Override
    public int cardinality(int orElse)
    {
        return 1;
    }

    @Override
    protected Object nativeSparkSqlRowValue(GenericInternalRow row, int position)
    {
        return null;
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int position)
    {
        return null;
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return null;
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setToNull(position);
    }
}
