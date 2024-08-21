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

package org.apache.cassandra.spark.data.converter.types.complex;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import org.apache.cassandra.bridge.BigNumberConfig;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.apache.cassandra.spark.data.converter.types.SparkType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public class Tuple implements CollectionTrait
{
    private final SparkSqlTypeConverter converter;
    private final CqlField.CqlTuple tuple;

    public Tuple(SparkSqlTypeConverter converter, CqlField.CqlTuple tuple)
    {
        this.converter = converter;
        this.tuple = tuple;
    }

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createStructType(IntStream.range(0, size())
                                                   .mapToObj(index -> DataTypes.createStructField(
                                                   Integer.toString(index),
                                                   sparkType(index).dataType(bigNumberConfig),
                                                   true))
                                                   .toArray(StructField[]::new)
        );
    }

    public CqlField.CqlCollection collection()
    {
        return tuple;
    }

    public SparkSqlTypeConverter converter()
    {
        return converter;
    }

    @Override
    public Object toSparkSqlType(Object value, boolean isFrozen)
    {
        if (value instanceof ByteBuffer)
        {
            // Need to deserialize first, e.g. if tuple is frozen inside collections
            return toSparkSqlType(tuple.deserializeToJava((ByteBuffer) value), isFrozen);
        }
        else
        {
            Object[] array = (Object[]) value;
            for (int index = 0; index < array.length; index++)
            {
                array[index] = array[index] == null ? null : sparkType(index).toSparkSqlType(array[index], isFrozen);
            }
            return new GenericInternalRow(array);
        }
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int position)
    {
        final InternalRow tupleStruct = row.getStruct(position, size());
        return IntStream.range(0, size())
                        .boxed()
                        .map(index -> sparkType(index).toTestRowType(tupleStruct.get(index, sparkType(index).dataType())))
                        .toArray();
    }

    @Override
    public Object sparkSqlRowValue(Row row, int position)
    {
        final Row tupleStruct = row.getStruct(position);
        return IntStream.range(0, tupleStruct.size())
                        .boxed()
                        .filter(index -> !tupleStruct.isNullAt(index))
                        .map(index -> sparkType(index).toTestRowType(tupleStruct.get(index)))
                        .toArray();
    }

    @Override
    public Object toTestRowType(Object value)
    {
        final GenericRowWithSchema tupleRow = (GenericRowWithSchema) value;
        final Object[] tupleResult = new Object[tupleRow.size()];
        for (int index = 0; index < tupleRow.size(); index++)
        {
            tupleResult[index] = sparkType(index).toTestRowType(tupleRow.get(index));
        }
        return tupleResult;
    }

    @Override
    public boolean equalsTo(Object first, Object second)
    {
        return SparkType.equalsArrays(((GenericInternalRow) first).values(), ((GenericInternalRow) second).values(), this::sparkType);
    }

    @Override
    public int compareTo(Object first, Object second)
    {
        return SparkType.compareArrays(((GenericInternalRow) first).values(), ((GenericInternalRow) second).values(), this::sparkType);
    }
}
