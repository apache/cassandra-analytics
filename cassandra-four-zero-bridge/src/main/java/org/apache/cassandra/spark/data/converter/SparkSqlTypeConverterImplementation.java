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

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.converter.types.Empty;
import org.apache.cassandra.spark.data.converter.types.SparkAscii;
import org.apache.cassandra.spark.data.converter.types.SparkBigInt;
import org.apache.cassandra.spark.data.converter.types.SparkBlob;
import org.apache.cassandra.spark.data.converter.types.SparkBoolean;
import org.apache.cassandra.spark.data.converter.types.SparkCounter;
import org.apache.cassandra.spark.data.converter.types.SparkDate;
import org.apache.cassandra.spark.data.converter.types.SparkDecimal;
import org.apache.cassandra.spark.data.converter.types.SparkDouble;
import org.apache.cassandra.spark.data.converter.types.SparkDuration;
import org.apache.cassandra.spark.data.converter.types.SparkFloat;
import org.apache.cassandra.spark.data.converter.types.SparkInet;
import org.apache.cassandra.spark.data.converter.types.SparkInt;
import org.apache.cassandra.spark.data.converter.types.SparkSmallInt;
import org.apache.cassandra.spark.data.converter.types.SparkText;
import org.apache.cassandra.spark.data.converter.types.SparkTime;
import org.apache.cassandra.spark.data.converter.types.SparkTimeUUID;
import org.apache.cassandra.spark.data.converter.types.SparkTimestamp;
import org.apache.cassandra.spark.data.converter.types.SparkTinyInt;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.cassandra.spark.data.converter.types.SparkUUID;
import org.apache.cassandra.spark.data.converter.types.SparkVarChar;
import org.apache.cassandra.spark.data.converter.types.SparkVarInt;
import org.apache.cassandra.spark.data.converter.types.complex.SparkFrozen;
import org.apache.cassandra.spark.data.converter.types.complex.SparkList;
import org.apache.cassandra.spark.data.converter.types.complex.SparkMap;
import org.apache.cassandra.spark.data.converter.types.complex.SparkSet;
import org.apache.cassandra.spark.data.converter.types.complex.SparkTuple;
import org.apache.cassandra.spark.data.converter.types.complex.SparkUdt;
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
        types.put(org.apache.cassandra.spark.data.types.Ascii.class, SparkAscii.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.BigInt.class, SparkBigInt.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Blob.class, SparkBlob.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Boolean.class, SparkBoolean.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Counter.class, SparkCounter.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Date.class, SparkDate.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Decimal.class, SparkDecimal.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Double.class, SparkDouble.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Duration.class, SparkDuration.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Empty.class, Empty.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Float.class, SparkFloat.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Inet.class, SparkInet.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Int.class, SparkInt.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.SmallInt.class, SparkSmallInt.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Text.class, SparkText.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Time.class, SparkTime.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.Timestamp.class, SparkTimestamp.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.TimeUUID.class, SparkTimeUUID.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.TinyInt.class, SparkTinyInt.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.UUID.class, SparkUUID.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.VarChar.class, SparkVarChar.INSTANCE);
        types.put(org.apache.cassandra.spark.data.types.VarInt.class, SparkVarInt.INSTANCE);
        NATIVE_TYPES = ImmutableMap.copyOf(types);
    }

    public SparkType toSparkType(CqlField.CqlType cqlType)
    {
        return getOrThrow(cqlType);
    }

    public Object convert(CqlField.CqlType cqlType, @NotNull Object value, boolean isFrozen)
    {
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
            return new SparkFrozen(INSTANCE, ((CqlField.CqlFrozen) cqlType).inner());
        }

        if (cqlType.isComplex())
        {
            if (cqlType instanceof CqlField.CqlSet)
            {
                return new SparkSet(INSTANCE, (CqlField.CqlSet) cqlType);
            }
            else if (cqlType instanceof CqlField.CqlList)
            {
                return new SparkList(INSTANCE, (CqlField.CqlList) cqlType);
            }
            else if (cqlType instanceof CqlField.CqlMap)
            {
                return new SparkMap(INSTANCE, (CqlField.CqlMap) cqlType);
            }
            else if (cqlType instanceof CqlField.CqlTuple)
            {
                return new SparkTuple(INSTANCE, (CqlField.CqlTuple) cqlType);
            }
            else if (cqlType instanceof CqlField.CqlUdt)
            {
                return new SparkUdt(INSTANCE, (CqlField.CqlUdt) cqlType);
            }
            throw new IllegalStateException("Unexpected complex type: " + cqlType);
        }

        return Objects.requireNonNull(nativeSparkType, String.format("No mapping from %s Cql native type to Spark type", cqlType.getClass().getName()));
    }
}
