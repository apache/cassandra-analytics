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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import static org.apache.cassandra.cdc.avro.AvroConstants.ARRAY_BASED_MAP_KEY_NAME;
import static org.apache.cassandra.cdc.avro.AvroConstants.ARRAY_BASED_MAP_VALUE_NAME;
import static org.apache.cassandra.cdc.avro.AvroSchemas.isRecordBasedUdt;
import static org.apache.cassandra.cdc.avro.AvroSchemas.unwrapNullable;

/**
 * Utils for handling Avro data for project internal use.
 */
public final class AvroDataUtils
{
    private AvroDataUtils()
    {
    }

    private static final Conversions.DecimalConversion decimalConversions = new Conversions.DecimalConversion();

    /**
     * Converts Cassandra value object to Avro, and eventually the Avro data is used to be converted into
     * the Spark format.
     * The available Cassandra data types can be found at [1]. Internally, the data types are mapped
     * to those Java types [2].
     * Although there are many data types supported by Avro [3], the data types in Avro that can be converted
     * into Spark are limited. The supported Avro to Spark conversion can be found at [4].
     * For java types of each CQL type appreicated in the Cassandra java driver, check out [5].
     * <p>
     * Therefore, the Cassandra to Avro data types mapping can be summarized as the following:
     * | Cassandra Type        | Java Type             | Avro Type
     * |-----------------------|-----------------------|-----------------------
     * | ascii                 | String                | string
     * | bigint                | Long                  | long
     * | blob                  | ByteBuffer            | bytes
     * | boolean               | Boolean               | boolean
     * | counter (not supported)
     * | date                  | Integer               | int (logical type: date)
     * | decimal               | BigDecimal            | fixed bytes (logical type: decimal)
     * | double                | Double                | double
     * | duration (not supported)
     * | empty (not supported)
     * | float                 | Float                 | float
     * | inet                  | InetAddress           | bytes (logical type: inet)
     * | int                   | Integer               | int
     * | smallint              | Short                 | int
     * | text                  | String                | string
     * | time                  | Long                  | long
     * | timestamp             | Date                  | long (logical type: timestamp)
     * | timeuuid              | UUID                  | string (logical type: uuid)
     * | tinyint               | Byte                  | int
     * | uuid                  | UUID                  | string (logical type: uuid)
     * | varchar               | String                | string
     * | varint                | BigInteger            | fixed bytes (logical type: decimal)
     * | list                  | List                  | array of records
     * | set                   | Set                   | array of records (logical type: array_set)
     * | map                   | Map                   | array of key-value records (logical type: array_map)
     * |-----------------------------------------------------------------------
     * Note that for Java List and Set, Avro treats them as Collection and converts to array.
     * <p>
     * [1]: https://cassandra.apache.org/doc/latest/cassandra/cql/types.html
     * [2]: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/cql3/CQL3Type.java
     * [3]: https://avro.apache.org/docs/1.11.1/specification/
     * [4]: https://spark.apache.org/docs/latest/sql-data-sources-avro.html#supported-types-for-avro---spark-sql-conversion
     * [5]: https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/#cql-to-java-type-mapping
     *
     * @param cassandraValue Cassandra value
     * @param fieldSchema    Avro schema for the field
     */
    public static Object toAvro(Object cassandraValue, Schema fieldSchema)
    {
        if (cassandraValue instanceof Byte)
        {
            return Integer.valueOf((Byte) cassandraValue);
        }
        else if (cassandraValue instanceof Short)
        {
            return Integer.valueOf((Short) cassandraValue);
        }
        else if (cassandraValue instanceof InetAddress)
        {
            return ByteBuffer.wrap(((InetAddress) cassandraValue).getAddress());
        }
        else if (cassandraValue instanceof Date)
        {
            Date date = (Date) cassandraValue;
            return TimeUnit.MILLISECONDS.toMicros(date.getTime());
        }
        else if (cassandraValue instanceof Map)
        {
            Map<Object, Object> map = (Map<Object, Object>) cassandraValue;
            Schema unwrapped = unwrapNullable(fieldSchema);
            if (isRecordBasedUdt(unwrapped))
            {
                // udt type
                GenericData.Record udtEntry = new GenericData.Record(unwrapped);
                map.forEach((key, value) -> {
                    final String fieldName = key.toString(); // UDT key should always be the fieldName of type String
                    udtEntry.put(fieldName, toAvro(value, unwrapped.getField(fieldName).schema()));
                });
                return udtEntry;
            }
            else
            {
                // map type
                return map.entrySet().stream().map(entry -> {
                    Schema mapEntrySchema = unwrapped.getElementType();
                    GenericData.Record mapEntry = new GenericData.Record(mapEntrySchema);
                    mapEntry.put(ARRAY_BASED_MAP_KEY_NAME,
                                 toAvro(entry.getKey(), mapEntrySchema.getField(ARRAY_BASED_MAP_KEY_NAME).schema()));
                    mapEntry.put(ARRAY_BASED_MAP_VALUE_NAME,
                                 toAvro(entry.getValue(), mapEntrySchema.getField(ARRAY_BASED_MAP_VALUE_NAME).schema()));
                    return mapEntry;
                }).collect(Collectors.toList());
            }
        }
        else if (cassandraValue instanceof Collection) // matches List and Set
        {
            Schema elementSchema = unwrapNullable(fieldSchema).getElementType();
            return ((Collection<?>) cassandraValue).stream()
                                                   .map(value -> toAvro(value, elementSchema))
                                                   .collect(Collectors.toList());
        }
        else if (cassandraValue instanceof UUID)
        {
            return cassandraValue.toString();
        }
        else if (cassandraValue instanceof BigInteger)
        {
            Schema schema = unwrapNullable(fieldSchema);
            // spark-avro converter set the field type as "FIXED"
            return decimalConversions.toFixed(new BigDecimal((BigInteger) cassandraValue), schema, schema.getLogicalType());
        }
        else if (cassandraValue instanceof BigDecimal)
        {
            // spark-avro converter set the field type as "FIXED"
            Schema schema = unwrapNullable(fieldSchema);
            return decimalConversions.toFixed((BigDecimal) cassandraValue, schema, schema.getLogicalType());
        }
        return cassandraValue;
    }

    public static byte[] encode(GenericDatumWriter<GenericRecord> writer, GenericData.Record update, BinaryEncoder encoder)
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, encoder);
        try
        {
            writer.write(update, binaryEncoder);
            binaryEncoder.flush();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }
}
