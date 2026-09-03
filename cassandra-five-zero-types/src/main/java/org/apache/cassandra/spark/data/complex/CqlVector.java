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

package org.apache.cassandra.spark.data.complex;

import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.data.CassandraTypes;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlType;
import org.apache.cassandra.utils.TimeUUID;
import org.jetbrains.annotations.NotNull;

public class CqlVector extends CqlCollection implements CqlField.CqlVector
{
    private final int dimensions;

    public CqlVector(CqlField.CqlType type, int dimensions)
    {
        super(type);
        this.dimensions = dimensions;
        this.hashCode = Objects.hash(this.hashCode, dimensions);
    }

    public static CqlVector read(Input input, CassandraTypes cassandraTypes)
    {
        int dimensions = input.readInt();
        CqlField.CqlType[] types = CqlCollection.readTypes(input, cassandraTypes);
        Preconditions.checkArgument(types.length == 1, "Unexpected number of vector subtypes: " + types.length);
        return new CqlVector(types[0], dimensions);
    }

    @Override
    public void write(Output output)
    {
        CqlField.CqlType.write(this, output);
        output.writeInt(dimensions);
        writeTypes(output);
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return VectorType.getInstance(((CqlType) type()).dataType(), dimensions);
    }

    @Override
    public InternalType internalType()
    {
        return InternalType.Vector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) dataType(false).getSerializer();
    }

    @Override
    public String name()
    {
        return "vector";
    }

    @Override
    public String cqlName()
    {
        return String.format("%s<%s, %d>",
                             internalType().name().toLowerCase(),
                             types.get(0).cqlName(),
                             dimensions);
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, @NotNull Object value)
    {
        List<?> vector = (List<?>) value;
        validate(vector);
        udtValue.setVector(position, vector);
    }

    @Override
    public Object randomValue(int minCollectionSize, Random random)
    {
        return IntStream.range(0, dimensions)
                        .mapToObj(element -> type().randomValue(minCollectionSize, random))
                        .collect(Collectors.toList());
    }

    @Override
    public org.apache.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return org.apache.cassandra.cql3.functions.types.DataType.vector(((CqlType) type()).driverDataType(isFrozen), dimensions);
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraVersion version, boolean isCollectionElement)
    {
        List<?> vector = (List<?>) value;
        validate(vector);
        return vector.stream()
                     .map(element -> type().convertForCqlWriter(element, version, true))
                     .collect(Collectors.toList());
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        CqlVector that = (CqlVector) o;
        return super.equals(o) && dimensions == that.dimensions;
    }

    protected CellPath randomCellPath()
    {
        return CellPath.create(TimeUUID.Generator.nextTimeUUID().toBytes());
    }

    private void validate(List<?> vector)
    {
        Preconditions.checkArgument(vector.size() == dimensions, "Expected " + dimensions + " for vector: " + vector);
    }
}
