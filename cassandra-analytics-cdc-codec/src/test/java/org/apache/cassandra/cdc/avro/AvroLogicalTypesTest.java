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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.cassandra.cdc.schemastore.LocalTableSchemaStore;
import org.apache.cassandra.cdc.schemastore.SchemaStore;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AvroLogicalTypesTest
{
    @BeforeAll
    public static void registerLogicalTypeConversions()
    {
        GenericData genericData = GenericData.get();
        genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());
        genericData.addLogicalTypeConversion(new Conversions.UUIDConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());
        // note: we won't use millis, but have it here just for completeness
        genericData.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        genericData.addLogicalTypeConversion(new TimeConversions.LocalTimestampMicrosConversion());
    }

    @AfterAll
    public static void unregister()
    {
        GenericData.get().getConversions().clear();
    }

    @Test
    public void testDecimal() throws IOException
    {
        // test_decimal.avsc uses the scale of 8
        BigDecimal testData = new BigDecimal("1234.56").setScale(8);
        testLogicalTypeDataWriteAndReadBack("decimal", testData);
    }

    @Test
    public void testUUID() throws IOException
    {
        testLogicalTypeDataWriteAndReadBack("uuid", UUID.randomUUID());
    }

    @Test
    public void testDate() throws IOException
    {
        testLogicalTypeDataWriteAndReadBack("date", LocalDate.now());
    }

    @Test
    public void testTimeMicros() throws IOException
    {
        testLogicalTypeDataWriteAndReadBack("time_micros", LocalTime.now());
    }

    @Test
    public void testTimestampMicros() throws IOException
    {
        testLogicalTypeDataWriteAndReadBack("timestamp_micros", Instant.now());
    }

    private <T> void testLogicalTypeDataWriteAndReadBack(String typeName, T testData) throws IOException
    {
        SchemaStore schemaStore = LocalTableSchemaStore.getInstance();
        Schema schema = schemaStore.getSchema("test." + typeName, null);

        GenericData.Record rec = new GenericData.Record(schema);
        rec.put("a", testData);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(0), null);
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, encoder);

        schemaStore.getWriter("test." + typeName, null)
                   .write(rec, binaryEncoder);
        binaryEncoder.flush();

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        GenericRecord payloadRecord = schemaStore.getReader("test." + typeName, null)
                                                 .read(null, decoder);

        Class<? extends T> type = (Class<? extends T>) testData.getClass();
        T actualData = getAs(payloadRecord, "a", type);
        assertThat(actualData.getClass()).isSameAs(type);
        assertThat(actualData).isEqualTo(testData);
    }

    public static <T> T getAs(GenericRecord avroRec, String fieldName, Class<T> type) throws AvroRuntimeException
    {
        Object v = avroRec.get(fieldName);
        if (!v.getClass().isAssignableFrom(type))
        {
            throw new AvroRuntimeException("Value is not " + type.getSimpleName()
                                           + " of field: " + fieldName);
        }
        return (T) v;
    }
}
