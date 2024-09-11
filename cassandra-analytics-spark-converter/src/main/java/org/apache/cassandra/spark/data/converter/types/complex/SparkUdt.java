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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

import java.nio.ByteBuffer;

public class SparkUdt implements SparkType
{
    private final SparkSqlTypeConverter converter;
    private final CqlField.CqlUdt udt;
    private final List<SparkType> sparkTypes;

    public SparkUdt(SparkSqlTypeConverter converter, CqlField.CqlUdt udt)
    {
        this.converter = converter;
        this.udt = udt;
        this.sparkTypes = udt.fields().stream()
                             .map(CqlField::type)
                             .map(converter::toSparkType)
                             .collect(Collectors.toList());
    }

    public CqlField field(int position)
    {
        return this.udt.field(position);
    }

    @Override
    public DataType dataType(BigNumberConfig bigNumberConfig)
    {
        return DataTypes.createStructType(
        udt.fields().stream()
           .map(field -> DataTypes.createStructField(field.name(),
                                                     converter.sparkSqlType(field.type(), bigNumberConfig),
                                                     true))
           .toArray(StructField[]::new)
        );
    }

    @Override
    public Object sparkSqlRowValue(GenericInternalRow row, int position)
    {
        InternalRow struct = row.getStruct(position, size());
        return IntStream.range(0, size())
                        .boxed()
                        .collect(Collectors.toMap(
                        index -> field(index).name(),
                        index -> sparkType(index).toTestRowType(struct.get(index, sparkType(index).dataType())
                        )));
    }

    @Override
    public Object sparkSqlRowValue(Row row, int position)
    {
        Row struct = row.getStruct(position);
        return IntStream.range(0, struct.size())
                        .boxed()
                        .filter(index -> !struct.isNullAt(index))
                        .collect(Collectors.toMap(
                        index -> struct.schema().fields()[index].name(),
                        index -> sparkType(index).toTestRowType(struct.get(index))
                        ));
    }

    public List<SparkType> sparkTypes()
    {
        return sparkTypes;
    }

    public int size()
    {
        return udt.fields().size();
    }

    public SparkType sparkType()
    {
        return sparkType(0);
    }

    public SparkType sparkType(int position)
    {
        return sparkTypes().get(position);
    }

    public int compareTo(Object first, Object second)
    {
        return SparkType.compareArrays(((GenericInternalRow) first).values(), ((GenericInternalRow) second).values(), this::sparkType);
    }

    @Override
    public boolean equals(Object first, Object second)
    {
        return SparkType.equalsArrays(((GenericInternalRow) first).values(), ((GenericInternalRow) second).values(), this::sparkType);
    }

    @Override
    public Object toTestRowType(Object value)
    {
        GenericRowWithSchema row = (GenericRowWithSchema) value;
        String[] fieldNames = row.schema().fieldNames();
        Map<String, Object> result = new LinkedHashMap<>(fieldNames.length);
        for (int index = 0; index < fieldNames.length; index++)
        {
            result.put(fieldNames[index], sparkType(index).toTestRowType(row.get(index)));
        }
        return result;
    }

    @Override
    public Object toSparkSqlType(Object value, boolean isFrozen)
    {
        return udtToSparkSqlType(value, isFrozen);
    }

    @SuppressWarnings("unchecked")
    private GenericInternalRow udtToSparkSqlType(Object value, boolean isFrozen)
    {
        if (value instanceof ByteBuffer)
        {
            // Need to deserialize first, e.g. if UDT is frozen inside collections
            return udtToSparkSqlType(udt.deserializeUdt(converter, (ByteBuffer) value, isFrozen));
        }
        else
        {
            if (!(value instanceof Map))
            {
                throw new IllegalArgumentException("Expected Map<String, Object> or raw ByteBuffer for UDT type");
            }
            // UDTs should be deserialized to Map<String, Object> by `CqlField.deserializeUdt`
            Map<String, Object> map = (Map<String, Object>) value;
            return udtToSparkSqlType(map);
        }
    }

    private GenericInternalRow udtToSparkSqlType(Map<String, Object> value)
    {
        Object[] objects = new Object[size()];
        for (int index = 0; index < size(); index++)
        {
            objects[index] = value.getOrDefault(field(index).name(), null);
        }
        return new GenericInternalRow(objects);
    }
}
