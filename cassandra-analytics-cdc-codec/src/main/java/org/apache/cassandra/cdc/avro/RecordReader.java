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

import java.util.NoSuchElementException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import javax.annotation.Nonnull;

import static org.apache.cassandra.cdc.avro.AvroSchemas.unwrapNullable;

/**
 * Reads values from {@link GenericRecord}
 * The value is converted, if there is a {@link TypeConversion} registered.
 * Otherwise, the value read from {@link GenericRecord} is returned as-is.
 */
public class RecordReader
{
    private static final RecordReader INSTANCE = new RecordReader();

    private final TypeConversion.Registry registry;

    public static RecordReader get()
    {
        return INSTANCE;
    }

    private RecordReader()
    {
        this.registry = new TypeConversionRegistry();
        registry.register(new TypeConversion.UUIDConversion());
        registry.register(new TypeConversion.DateConversion());
        registry.register(new TypeConversion.DecimalConversion());
        registry.register(new TypeConversion.TimestampConversion());
        registry.register(new TypeConversion.VarIntConversion());
        registry.register(new TypeConversion.InetAddressConversion());
        registry.register(new TypeConversion.SetConversion());
        registry.register(new TypeConversion.MapConversion());
        registry.register(new TypeConversion.ListConversion());
        registry.register(new TypeConversion.UdtConversion());
    }

    /**
     * Read the value of the field. Conversion is applied (if applicable for the field) before returning.
     * For example, a field is read as string with the UUID logical type, it is converted to a UUID and returned.
     * See {@link #RecordReader()} for all the registered conversions.
     *
     * @param genericRecord data container to read from
     * @param fieldName     field to read
     * @return value of the field
     */
    public Object read(GenericRecord genericRecord, String fieldName)
    {
        if (!genericRecord.hasField(fieldName))
        {
            throw new NoSuchElementException("No such field: " + fieldName);
        }

        Object fieldValue = genericRecord.get(fieldName);
        if (fieldValue == null)
        {
            return null;
        }

        Schema fieldSchema = genericRecord.getSchema()
                                          .getField(fieldName)
                                          .schema();
        return convert(unwrapNullable(fieldSchema), fieldValue);
    }

    /**
     * Convert the field value according to the field scheme.
     * If there is a conversion registered, the conversion is applied for the value.
     * Otherwise, the field value is returned untouched.
     *
     * @param fieldSchema Avro schema for the field
     * @param fieldValue  field value
     * @return the converted value or the input as-is.
     */
    Object convert(Schema fieldSchema, @Nonnull Object fieldValue)
    {
        TypeConversion<?> typeConversion = registry.lookup(fieldSchema);

        if (typeConversion != null)
        {
            return typeConversion.convert(fieldSchema, fieldValue);
        }
        // avro stores string values in Utf8, we need to convert it to string to use.
        else if (fieldValue instanceof Utf8)
        {
            return fieldValue.toString();
        }

        // no conversion is applicable, return the value as-is.
        return fieldValue;
    }
}
