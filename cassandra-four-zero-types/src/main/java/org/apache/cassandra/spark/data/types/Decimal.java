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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.spark.data.NativeType;
import org.apache.cassandra.spark.utils.RandomUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class Decimal extends NativeType
{
    public static final Decimal INSTANCE = new Decimal();
    private static final Comparator<org.apache.spark.sql.types.Decimal> DECIMAL_COMPARATOR = Comparator.naturalOrder();

    @Override
    public String name()
    {
        return "decimal";
    }

    @Override
    public DataType sparkSqlType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createDecimalType(bigNumberConfig.bigDecimalPrecision(), bigNumberConfig.bigDecimalScale());
    }

    @Override
    public AbstractType<?> dataType()
    {
        return DecimalType.instance;
    }

    @Override
    public Object toSparkSqlType(Object value, boolean isFrozen)
    {
        return org.apache.spark.sql.types.Decimal.apply((BigDecimal) value);
    }

    @Override
    protected int compareTo(Object first, Object second)
    {
        return DECIMAL_COMPARATOR.compare((org.apache.spark.sql.types.Decimal) first, (org.apache.spark.sql.types.Decimal) second);
    }

    @Override
    protected Object nativeSparkSqlRowValue(GenericInternalRow row, int position)
    {
        return row.getDecimal(position, BigNumberConfig.DEFAULT.bigIntegerPrecision(), BigNumberConfig.DEFAULT.bigIntegerScale());
    }

    @Override
    protected Object nativeSparkSqlRowValue(Row row, int position)
    {
        return row.getDecimal(position);
    }

    @Override
    public Object toTestRowType(Object value)
    {
        if (value instanceof BigDecimal)
        {
            return value;
        }
        return ((org.apache.spark.sql.types.Decimal) value).toJavaBigDecimal();
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        BigInteger unscaledVal = new BigInteger(BigNumberConfig.DEFAULT.bigDecimalPrecision(), RandomUtils.RANDOM);
        int scale = BigNumberConfig.DEFAULT.bigDecimalScale();
        return new BigDecimal(unscaledVal, scale);
    }

    @Override
    public void setInnerValue(SettableByIndexData<?> udtValue, int position, Object value)
    {
        udtValue.setDecimal(position, (BigDecimal) value);
    }

    @Override
    public org.apache.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.cql3.functions.types.DataType.decimal();
    }
}
