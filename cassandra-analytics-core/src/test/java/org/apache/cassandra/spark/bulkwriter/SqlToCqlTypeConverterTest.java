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

package org.apache.cassandra.spark.bulkwriter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.spark.data.CqlField;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;

import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.ASCII;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BIGINT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BLOB;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.BOOLEAN;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.COUNTER;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DATE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DECIMAL;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DOUBLE;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.DURATION;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.FLOAT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INET;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.INT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.SMALLINT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TEXT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIME;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIMESTAMP;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TIMEUUID;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.TINYINT;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.UUID;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARCHAR;
import static org.apache.cassandra.spark.bulkwriter.SqlToCqlTypeConverter.VARINT;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCqlCustom;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockListCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockMapCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockSetCqlType;
import static org.apache.cassandra.spark.bulkwriter.TableSchemaTestCommon.mockUdtCqlType;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public final class SqlToCqlTypeConverterTest
{

    public static Collection<Object[]> dataTypes()
    {
        return Arrays.asList(na(mockCqlType(BIGINT), SqlToCqlTypeConverter.LongConverter.class),
                             na(mockCqlType(BLOB), SqlToCqlTypeConverter.BytesConverter.class),
                             na(mockListCqlType(INT), SqlToCqlTypeConverter.ListConverter.class),
                             na(mockCqlType(DECIMAL), SqlToCqlTypeConverter.BigDecimalConverter.class),
                             na(mockCqlType(TIMESTAMP), SqlToCqlTypeConverter.TimestampConverter.class),
                             na(mockCqlType(TIME), SqlToCqlTypeConverter.TimeConverter.class),
                             na(mockCqlType(UUID), SqlToCqlTypeConverter.UUIDConverter.class),
                             na(mockCqlType(VARINT), SqlToCqlTypeConverter.BigIntegerConverter.class),
                             na(mockCqlType(TIMEUUID), SqlToCqlTypeConverter.TimeUUIDConverter.class),
                             na(mockCqlType(INET), SqlToCqlTypeConverter.InetAddressConverter.class),
                             na(mockCqlType(DATE), SqlToCqlTypeConverter.DateConverter.class),
                             na(mockCqlType(DURATION), SqlToCqlTypeConverter.DurationConverter.class),
                             na(mockMapCqlType(INT, INT), SqlToCqlTypeConverter.MapConverter.class),
                             na(mockSetCqlType(INET), SqlToCqlTypeConverter.SetConverter.class),
                             na(mockUdtCqlType("udtType", "f1", TEXT, "f2", INT, "f3", TIMEUUID), SqlToCqlTypeConverter.UdtConverter.class),
                             // Special Cassandra 1.2 Timestamp type should use TimestampConverter
                             na(mockCqlCustom("org.apache.cassandra.db.marshal.DateType"), SqlToCqlTypeConverter.TimestampConverter.class));
    }


    @ParameterizedTest(name = "{index}: {0} -> {1}")
    @MethodSource("dataTypes")
    public void testConverter(CqlField.CqlType dt, Class<?> expectedConverter)
    {
        assertConverterType(dt, expectedConverter);
    }

    private static Object[] na(CqlField.CqlType dataType, Class<?> converter)
    {
        return new Object[]{dataType, converter};
    }

    public static Collection<Object[]> noOpDataTypes()
    {
        return Arrays.asList(na(mockCqlType(ASCII)),
                             na(mockCqlType(BOOLEAN)),
                             na(mockCqlType(COUNTER)),
                             na(mockCqlType(DOUBLE)),
                             na(mockCqlType(FLOAT)),
                             na(mockCqlType(TEXT)),
                             na(mockCqlType(VARCHAR)),
                             na(mockCqlType(SMALLINT)),
                             na(mockCqlType(TINYINT)));
    }

    private static Object[] na(CqlField.CqlType dataType)
    {
        return new Object[]{dataType};
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("noOpDataTypes")
    public void testNoOpTypes(CqlField.CqlType dataType)
    {
        assertConverterType(dataType, SqlToCqlTypeConverter.NoOp.class);
    }

    @Test
    public void testVarIntFromDecimal()
    {
        // The bulk reader returns varint values as Spark decimals, which reach the writer as BigDecimal
        SqlToCqlTypeConverter.Converter<?> converter = SqlToCqlTypeConverter.getConverter(mockCqlType(VARINT));
        BigInteger expected = new BigInteger("-98765432109876543210");

        assertThat(converter.convert(new BigDecimal(expected)), equalTo(expected));
        assertThat(converter.convert(new scala.math.BigDecimal(new BigDecimal(expected))), equalTo(expected));
        assertThat(converter.convert(new BigDecimal("42.000")), equalTo(BigInteger.valueOf(42)));
    }

    @Test
    public void testVarIntRejectsFractionalDecimal()
    {
        SqlToCqlTypeConverter.Converter<?> converter = SqlToCqlTypeConverter.getConverter(mockCqlType(VARINT));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> converter.convert(new BigDecimal("1.5")));
        assertThat(exception.getMessage(), containsString("VARINT"));
    }

    @Test
    public void testDateFromSparkDateInAnyTimeZone()
    {
        // Spark passes a date to the writer as a java.sql.Date at midnight in the time zone of the JVM, in the hybrid
        // Julian and Gregorian calendar, while Cassandra stores proleptic Gregorian days
        List<LocalDate> dates = Arrays.asList(LocalDate.of(2024, 2, 1),
                                              LocalDate.of(1969, 12, 31),
                                              LocalDate.of(1500, 3, 1),
                                              LocalDate.of(0, 1, 1),
                                              LocalDate.of(-1, 6, 15));
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try
        {
            for (String timeZone : Arrays.asList("UTC", "Europe/Madrid", "Asia/Tokyo", "America/Los_Angeles"))
            {
                TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
                SqlToCqlTypeConverter.Converter<?> converter = SqlToCqlTypeConverter.getConverter(mockCqlType(DATE));
                for (LocalDate date : dates)
                {
                    int days = Math.toIntExact(date.toEpochDay());
                    java.sql.Date sparkDate = DateTimeUtils.toJavaDate(days);

                    assertThat(timeZone + " " + date, converter.convert(sparkDate), equalTo(days - Integer.MIN_VALUE));
                }
            }
        }
        finally
        {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    @Test
    public void testDateFromLocalDate()
    {
        // Spark passes LocalDate values when spark.sql.datetime.java8API.enabled is set
        SqlToCqlTypeConverter.Converter<?> converter = SqlToCqlTypeConverter.getConverter(mockCqlType(DATE));
        LocalDate date = LocalDate.of(2024, 2, 1);

        assertThat(converter.convert(date), equalTo((int) date.toEpochDay() - Integer.MIN_VALUE));
    }

    @Test
    public void testInetFromAddressBytes() throws UnknownHostException
    {
        // The bulk reader returns inet values as the bytes of the address
        SqlToCqlTypeConverter.Converter<?> converter = SqlToCqlTypeConverter.getConverter(mockCqlType(INET));

        assertThat(converter.convert(new byte[]{127, 0, 0, 1}), equalTo(InetAddress.getByName("127.0.0.1")));
        assertThat(converter.convert(InetAddress.getByName("2001:db8::1").getAddress()),
                   equalTo(InetAddress.getByName("2001:db8::1")));
    }

    @Test
    public void testInetRejectsInvalidAddressBytes()
    {
        SqlToCqlTypeConverter.Converter<?> converter = SqlToCqlTypeConverter.getConverter(mockCqlType(INET));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> converter.convert(new byte[]{1, 2, 3}));
        assertThat(exception.getMessage(), containsString("INET"));
    }

    public static void assertConverterType(CqlField.CqlType dataType, Class expectedType)
    {
        assertThat(SqlToCqlTypeConverter.getConverter(dataType), instanceOf(expectedType));
    }
}
