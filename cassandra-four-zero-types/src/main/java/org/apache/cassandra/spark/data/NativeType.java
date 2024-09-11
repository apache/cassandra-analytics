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

package org.apache.cassandra.spark.data;

import java.util.Collections;
import java.util.Set;

import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.serializers.TypeSerializer;

public abstract class NativeType extends CqlType implements CqlField.NativeType
{
    private final int hashCode;

    protected NativeType()
    {
        hashCode = name().hashCode();
    }

    public CqlField.CqlType.InternalType internalType()
    {
        return CqlField.CqlType.InternalType.NativeCql;
    }

    @Override
    public boolean isSupported()
    {
        return true;
    }

    @Override
    public AbstractType<?> dataType()
    {
        throw CqlField.notImplemented(this);
    }

    @Override
    public Object convertForCqlWriter(Object value, CassandraVersion version)
    {
        return value;
    }

    @Override
    public AbstractType<?> dataType(boolean isMultiCell)
    {
        return dataType();
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    @Override
    public boolean equals(Object other)
    {
        return other != null && (this == other || this.getClass() == other.getClass());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TypeSerializer<T> serializer()
    {
        return (TypeSerializer<T>) dataType().getSerializer();
    }

    @Override
    public String cqlName()
    {
        return name().toLowerCase();
    }

    @Override
    public void write(Output output)
    {
        CqlField.CqlType.write(this, output);
        output.writeString(name());
    }

    public Set<CqlField.CqlUdt> udts()
    {
        return Collections.emptySet();
    }
}
