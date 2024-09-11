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

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;

import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cql3.functions.types.SettableByIndexData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlType;

public class CqlFrozen extends CqlType implements CqlField.CqlFrozen
{
    private final CqlField.CqlType inner;
    private final int hashCode;

    public CqlFrozen(CqlField.CqlType inner)
    {
        this.inner = inner;
        this.hashCode = Objects.hash(internalType().ordinal(), inner);
    }

    public static CqlFrozen build(CqlField.CqlType inner)
    {
        return new CqlFrozen(inner);
    }

    @Override
    public boolean isSupported()
    {
        return true;
    }

    @Override
    public AbstractType<?> dataType()
    {
        return ((CqlType) inner()).dataType(false);  // If frozen collection then isMultiCell is false
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return dataType();
    }

    @Override
    public <T> TypeSerializer<T> serializer()
    {
        return ((CqlType) inner()).serializer();
    }

    @Override
    public Object deserializeToJavaType(ByteBuffer buffer, boolean isFrozen)
    {
        return inner().deserializeToJavaType(buffer, isFrozen);
    }

    @Override
    public ByteBuffer serialize(Object value)
    {
        return inner().serialize(value);
    }

    public InternalType internalType()
    {
        return InternalType.Frozen;
    }

    @Override
    public String name()
    {
        return "frozen";
    }

    public CqlField.CqlType inner()
    {
        return inner;
    }

    public String cqlName()
    {
        return String.format("frozen<%s>", inner.cqlName());
    }

    @Override
    public Set<CqlField.CqlUdt> udts()
    {
        return inner.udts();
    }

    @Override
    protected void setInnerValueInternal(SettableByIndexData<?> udtValue, int position, Object value)
    {
        ((CqlType) inner()).setInnerValue(udtValue, position, value);
    }

    @Override
    public Object randomValue(int minCollectionSize)
    {
        return inner.randomValue(minCollectionSize);
    }

    @Override
    public org.apache.cassandra.cql3.functions.types.DataType driverDataType(boolean isFrozen)
    {
        return ((CqlType) inner()).driverDataType(true);
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraVersion version)
    {
        return inner.convertForCqlWriter(value, version);
    }

    @Override
    public void write(Output output)
    {
        CqlField.CqlType.write(this, output);
        inner.write(output);
    }

    @Override
    public String toString()
    {
        return cqlName();
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return false;
        }
        if (this == other)
        {
            return true;
        }
        if (this.getClass() != other.getClass())
        {
            return false;
        }

        CqlFrozen that = (CqlFrozen) other;
        return this.internalType() == that.internalType()
               && Objects.equals(this.inner, that.inner);
    }
}
