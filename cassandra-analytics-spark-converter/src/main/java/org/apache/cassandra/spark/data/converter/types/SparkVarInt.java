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

package org.apache.cassandra.spark.data.converter.types;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;

public class SparkVarInt implements DecimalFeatures
{
    public static final SparkVarInt INSTANCE = new SparkVarInt();

    private SparkVarInt()
    {

    }

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createDecimalType(bigNumberConfig.bigIntegerPrecision(), bigNumberConfig.bigIntegerScale());
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof BigInteger)
        {
            return value;
        }
        else if (value instanceof BigDecimal)
        {
            return ((BigDecimal) value).toBigInteger();
        }
        return ((org.apache.spark.sql.types.Decimal) value).toJavaBigInteger();
    }

    @Override
    public Object toSparkSqlType(@NotNull Object value, boolean isFrozen)
    {
        return org.apache.spark.sql.types.Decimal.apply((BigInteger) value);
    }

    @Override
    public Object nativeSparkSqlRowValue(final GenericInternalRow row, final int position)
    {
        return row.getDecimal(position, BigNumberConfig.DEFAULT.bigIntegerPrecision(), BigNumberConfig.DEFAULT.bigIntegerScale());
    }

    @Override
    public Object nativeSparkSqlRowValue(Row row, int position)
    {
        return row.getDecimal(position).toBigInteger();
    }
}
