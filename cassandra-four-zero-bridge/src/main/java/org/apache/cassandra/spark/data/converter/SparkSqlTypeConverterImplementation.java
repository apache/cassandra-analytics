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

package org.apache.cassandra.spark.data.converter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.converter.types.Ascii;
import org.apache.cassandra.spark.data.converter.types.BigInt;
import org.apache.cassandra.spark.data.converter.types.Blob;
import org.apache.cassandra.spark.data.converter.types.Boolean;
import org.apache.cassandra.spark.data.converter.types.Counter;
import org.apache.cassandra.spark.data.converter.types.Date;
import org.apache.cassandra.spark.data.converter.types.Decimal;
import org.apache.cassandra.spark.data.converter.types.Double;
import org.apache.cassandra.spark.data.converter.types.Duration;
import org.apache.cassandra.spark.data.converter.types.Empty;
import org.apache.cassandra.spark.data.converter.types.Float;
import org.apache.cassandra.spark.data.converter.types.Inet;
import org.apache.cassandra.spark.data.converter.types.Int;
import org.apache.cassandra.spark.data.converter.types.SmallInt;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.cassandra.spark.data.converter.types.Text;
import org.apache.cassandra.spark.data.converter.types.Time;
import org.apache.cassandra.spark.data.converter.types.TimeUUID;
import org.apache.cassandra.spark.data.converter.types.Timestamp;
import org.apache.cassandra.spark.data.converter.types.TinyInt;
import org.apache.cassandra.spark.data.converter.types.UUID;
import org.apache.cassandra.spark.data.converter.types.VarChar;
import org.apache.cassandra.spark.data.converter.types.VarInt;
import org.apache.cassandra.spark.data.converter.types.complex.Frozen;
import org.apache.cassandra.spark.data.converter.types.complex.List;
import org.apache.cassandra.spark.data.converter.types.complex.Udt;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.jetbrains.annotations.NotNull;

/**
 * SparkSqlTypeConverter implementation that maps Cassandra 4.0 types to SparkSQL data types.
 */
public class SparkSqlTypeConverterImplementation implements SparkSqlTypeConverter
{

    public static final SparkSqlTypeConverterImplementation INSTANCE = new SparkSqlTypeConverterImplementation();
    /*
            SparkSQL      |    Java
            ByteType      |    byte or Byte
            ShortType     |    short or Short
            IntegerType   |    int or Integer
            LongType      |    long or Long
            FloatType     |    float or Float
            DoubleType    |    double or Double
            DecimalType   |    java.math.BigDecimal
            StringType    |    String
            BinaryType    |    byte[]
            BooleanType   |    boolean or Boolean
            TimestampType |    java.sql.Timestamp
            DateType      |    java.sql.Date
            ArrayType     |    java.util.List
            MapType       |    java.util.Map

            See: https://spark.apache.org/docs/latest/sql-reference.html
        */
    public static final Map<Class<? extends CqlField.CqlType>, SparkType> NATIVE_TYPES;

    public Object nativeSparkSqlRowValue(CqlField.CqlType cqlType, GenericInternalRow row, int position)
    {
        return toSparkType(cqlType).nativeSparkSqlRowValue(row, position);
    }

    public Object nativeSparkSqlRowValue(CqlField.CqlType cqlType, Row row, int position)
    {
        return toSparkType(cqlType).nativeSparkSqlRowValue(row, position);
    }

    public Object sparkSqlRowValue(CqlField.CqlType cqlType, GenericInternalRow row, int position)
    {
        return toSparkType(cqlType).sparkSqlRowValue(row, position);
    }

    public Object sparkSqlRowValue(CqlField.CqlType cqlType, Row row, int position)
    {
        return toSparkType(cqlType).sparkSqlRowValue(row, position);
    }

    public DataType sparkSqlType(CqlField.CqlType cqlType, BigNumberConfig bigNumberConfig)
    {
        return toSparkType(cqlType).dataType(bigNumberConfig);
    }

    static
    {
        Map<Class<? extends CqlField.CqlType>, SparkType> types = new HashMap<>();
        types.put(org.apache.cassandra.spark.data.types.Ascii.class, Ascii.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.BigInt.class, BigInt.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Blob.class, Blob.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Boolean.class, Boolean.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Counter.class, Counter.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Date.class, Date.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Decimal.class, Decimal.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Double.class, Double.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Duration.class, Duration.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Empty.class, Empty.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Float.class, Float.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Inet.class, Inet.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Int.class, Int.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.SmallInt.class, SmallInt.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Text.class, Text.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Time.class, Time.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Timestamp.class, Timestamp.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.TimeUUID.class, TimeUUID.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.TinyInt.class, TinyInt.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.UUID.class, UUID.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.VarChar.class, VarChar.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.VarInt.class, VarInt.INSTANCE);
        NATIVE_TYPES = Map.copyOf(types);
    }

    public SparkType toSparkType(CqlField.CqlType cqlType)
    {
        return getOrThrow(cqlType);
    }

    public Object convert(CqlField.CqlType cqlType, @NotNull Object value, boolean isFrozen)
    {
        if (value == null)
        {
            return null;
        }
        return getOrThrow(cqlType).toSparkSqlType(value, isFrozen);
    }

    protected static SparkType getOrThrow(CqlField.CqlType cqlType)
    {
        if (cqlType == null)
        {
            throw new NullPointerException("Null CqlType provided");
        }

        SparkType nativeSparkType = NATIVE_TYPES.get(cqlType.getClass());
        if (nativeSparkType != null)
        {
            return nativeSparkType;
        }

        if (cqlType.isFrozen())
        {
            // frozen type has no equivalent in SparkSQL so unpack inner
            return new Frozen(INSTANCE, ((CqlField.CqlFrozen) cqlType).inner());
        }

        if (cqlType.isComplex())
        {
            if (cqlType instanceof CqlField.CqlSet)
            {
                return new org.apache.cassandra.spark.data.converter.types.complex.Set(INSTANCE, (CqlField.CqlSet) cqlType);
            }
            else if (cqlType instanceof CqlField.CqlList)
            {
                return new List(INSTANCE, (CqlField.CqlList) cqlType);
            }
            else if (cqlType instanceof CqlField.CqlMap)
            {
                return new org.apache.cassandra.spark.data.converter.types.complex.Map(INSTANCE, (CqlField.CqlMap) cqlType);
            }
            else if (cqlType instanceof CqlField.CqlTuple)
            {
                return new org.apache.cassandra.spark.data.converter.types.complex.Tuple(INSTANCE, (CqlField.CqlTuple) cqlType);
            }
            else if (cqlType instanceof CqlField.CqlUdt)
            {
                return new Udt(INSTANCE, (CqlField.CqlUdt) cqlType);
            }
            throw new IllegalStateException("Unexpected complex type: " + cqlType);
        }

        return Objects.requireNonNull(nativeSparkType, String.format("No mapping from %s Cql native type to Spark type", cqlType.getClass().getName()));
    }
}
