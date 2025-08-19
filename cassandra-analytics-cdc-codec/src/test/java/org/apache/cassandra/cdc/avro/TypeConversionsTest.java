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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TypeConversionsTest
{
    @Test
    public void testDateConversion()
    {
        TypeConversion.DateConversion dateConversion = new TypeConversion.DateConversion();
        LocalDate result = dateConversion.convert(null, 1234);
        assertThat(result).as("Date conversion result should match expected local date").isEqualTo(LocalDate.ofEpochDay(1234));
    }

    @Test
    public void testDateConversionTypeMismatch()
    {
        TypeConversion.DateConversion dateConversion = new TypeConversion.DateConversion();
        assertThatThrownBy(() -> dateConversion.convert(null, 1234L))
            .as("DateConversion should throw IllegalArgumentException for Long input")
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("DateConversion expects Integer input, but has Long");
    }

    @Test
    public void testUuidConversionFromString()
    {
        TypeConversion.UUIDConversion uuidConversion = new TypeConversion.UUIDConversion();
        UUID testUUID = UUID.randomUUID();
        // convert string
        UUID result = uuidConversion.convert(null, testUUID.toString());
        assertThat(result).as("UUID conversion from string should match original UUID").isEqualTo(testUUID);
    }

    @Test
    public void testUuidConversionFromUtf8()
    {
        TypeConversion.UUIDConversion uuidConversion = new TypeConversion.UUIDConversion();
        UUID testUUID = UUID.randomUUID();
        // convert string
        UUID result = uuidConversion.convert(null, new Utf8(testUUID.toString()));
        assertThat(result).as("UUID conversion from Utf8 should match original UUID").isEqualTo(testUUID);
    }

    @Test
    public void testTimeUuidConversionFromString()
    {
        String uuidStr = "203366be-7209-443b-adba-9f2cef199454";
        TypeConversion.UUIDConversion uuidConversion = new TypeConversion.UUIDConversion();
        UUID result = uuidConversion.convert(null, uuidStr);
        assertThat(result.version()).as("UUID version should be 4").isEqualTo(4);
        assertThat(result.toString()).as("UUID string representation should match input").isEqualTo(uuidStr);
    }

    @Test
    public void testUuidConversionTypeMismatch()
    {
        TypeConversion.UUIDConversion uuidConversion = new TypeConversion.UUIDConversion();
        assertThatThrownBy(() -> uuidConversion.convert(null, 1234L))
            .as("UUIDConversion should throw IllegalArgumentException for Long input")
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("UUIDConversion expects String input, but has Long");
    }

    @Test
    public void testInetAddressConversion() throws Exception
    {
        TypeConversion.InetAddressConversion conversion = new TypeConversion.InetAddressConversion();
        InetAddress testData = InetAddress.getByName("127.0.0.1");
        InetAddress result = conversion.convert(null, ByteBuffer.wrap(testData.getAddress()));
        assertThat(result).as("InetAddress conversion should match expected address").isEqualTo(testData);
    }

    @Test
    public void testVarIntConversion()
    {
        TypeConversion.VarIntConversion conversion = new TypeConversion.VarIntConversion();
        BigInteger testData = BigInteger.valueOf(12345L);
        Schema varint = LogicalTypes.decimal(5, 0).addToSchema(SchemaBuilder.fixed("fixed").size(5));
        AvroSchemas.flagCqlType(varint, "varint");
        BigInteger result = conversion.convert(varint, new GenericData.Fixed(varint, testData.toByteArray()));
        assertThat(result).as("VarInt conversion should match expected value").isEqualTo(testData);
    }

    @Test
    public void testInvalidTypeForVarInt()
    {
        TypeConversion.VarIntConversion conversion = new TypeConversion.VarIntConversion();
        BigDecimal testData = BigDecimal.valueOf(1234.56789);
        Schema invalidVarint = LogicalTypes.decimal(5, 5).addToSchema(SchemaBuilder.fixed("fixed").size(5));
        AvroSchemas.flagCqlType(invalidVarint, "varint");
        assertThatThrownBy(() -> conversion.convert(invalidVarint, new GenericData.Fixed(invalidVarint, testData.unscaledValue().toByteArray())))
            .as("VarIntConversion should throw IllegalStateException for invalid varint type")
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Not a valid varint type");
    }

    @Test
    public void testDecimalConversion()
    {
        TypeConversion.DecimalConversion conversion = new TypeConversion.DecimalConversion();
        BigDecimal testData = BigDecimal.valueOf(1234.56789);
        Schema decimal = LogicalTypes.decimal(5, 5).addToSchema(SchemaBuilder.fixed("fixed").size(5));
        BigDecimal result = conversion.convert(decimal, new GenericData.Fixed(decimal, testData.unscaledValue().toByteArray()));
        assertThat(result).as("Decimal conversion should match expected value").isEqualTo(testData);
    }

    @Test
    public void testTimestampConversion()
    {
        TypeConversion.TimestampConversion conversion = new TypeConversion.TimestampConversion();
        long testDataInMicros = 1000000L;
        Date result = conversion.convert(null, testDataInMicros);
        Date testData = new Date(TimeUnit.MICROSECONDS.toMillis(testDataInMicros));
        assertThat(result).as("Timestamp conversion should match expected date").isEqualTo(testData);
    }
}
