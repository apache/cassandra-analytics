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

package org.apache.cassandra.cdc.avro;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.cassandra.cdc.avro.TypeConversion.TypeMapping;

/**
 * {@inheritDoc}
 */
public class TypeConversionRegistry implements TypeConversion.Registry
{
    private final Map<TypeMapping, TypeConversion<?>> conversions = new HashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(TypeConversion<?> typeConversion)
    {
        conversions.put(typeConversion.typeMapping(), typeConversion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypeConversion<?> lookup(Schema fieldSchema)
    {
        LogicalType logicalType = fieldSchema.getLogicalType();
        if (logicalType == null)
        {
            // for a simple array/list, we need to run the list identity conversion to convert any subtypes
            return fieldSchema.getType() == Schema.Type.ARRAY
                   ? conversions.get(TypeConversion.ListConversion.LIST_IDENTITY_MAPPING)
                   : null;
        }

        TypeMapping mapping;
        // todo: refactor the ugly comparison
        if (fieldSchema.getType() == Schema.Type.ARRAY || Objects.equals(logicalType.getName(), "uuid"))
        {
            mapping = TypeMapping.of(fieldSchema.getType().getName(), logicalType.getName());
        }
        else if (fieldSchema.getType() == Schema.Type.RECORD)
        {
            mapping = TypeMapping.of(fieldSchema.getType().getName(), logicalType.getName());
        }
        else
        {
            mapping = TypeMapping.of(fieldSchema.getType().getName(), logicalType.getName(), AvroSchemas.cqlType(fieldSchema));
        }
        return conversions.get(mapping);
    }
}
