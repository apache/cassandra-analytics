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

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.spark.sql.types.Decimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

public class CqlFieldComparators extends VersionRunner
{
    public CqlFieldComparators(CassandraVersion version)
    {
        super(version);
    }

    private static CqlField createField(CqlField.CqlType type)
    {
        return new CqlField(false, false, false, "a", type, 0);
    }

    @Test
    public void testStringComparator()
    {
        // ASCII
        assertTrue(createField(bridge.ascii()).compare("a", "b") < 0);
        assertEquals(0, createField(bridge.ascii()).compare("b", "b"));
        assertTrue(createField(bridge.ascii()).compare("c", "b") > 0);
        assertTrue(createField(bridge.ascii()).compare("b", "a") > 0);

        assertTrue(createField(bridge.ascii()).compare("1", "2") < 0);
        assertEquals(0, createField(bridge.ascii()).compare("2", "2"));
        assertTrue(createField(bridge.ascii()).compare("3", "2") > 0);
        assertTrue(createField(bridge.ascii()).compare("2", "1") > 0);

        // TIMEUUID
        assertTrue(createField(bridge.timeuuid()).compare("856f3600-8d57-11e9-9298-798dbb8bb043", "7a146960-8d57-11e9-94f8-1763d9f66f5e") < 0);
        assertTrue(createField(bridge.timeuuid()).compare("964116b0-8d57-11e9-8097-5f40ae53943c", "8ebe0600-8d57-11e9-b507-7769fecef72d") > 0);
        assertEquals(0, createField(bridge.timeuuid()).compare("9dda9590-8d57-11e9-9906-8b25b9c1ff19", "9dda9590-8d57-11e9-9906-8b25b9c1ff19"));

        // UUID
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID larger = uuid1.compareTo(uuid2) >= 0 ? uuid1 : uuid2;
        UUID smaller = uuid1.compareTo(uuid2) <= 0 ? uuid1 : uuid2;
        assertTrue(createField(bridge.uuid()).compare(smaller, larger) < 0);
        assertTrue(createField(bridge.uuid()).compare(larger, smaller) > 0);
        assertEquals(0, createField(bridge.uuid()).compare(smaller, smaller));
        assertEquals(0, createField(bridge.uuid()).compare(larger, larger));

        // TEXT
        assertTrue(createField(bridge.text()).compare("abc", "abd") < 0);
        assertTrue(createField(bridge.text()).compare("abd", "abc") > 0);
        assertEquals(0, createField(bridge.text()).compare("abc", "abc"));
        assertEquals(0, createField(bridge.text()).compare("abd", "abd"));

        // VARCHAR
        assertTrue(createField(bridge.varchar()).compare("abc", "abd") < 0);
        assertTrue(createField(bridge.varchar()).compare("abd", "abc") > 0);
        assertEquals(0, createField(bridge.varchar()).compare("abc", "abc"));
        assertEquals(0, createField(bridge.varchar()).compare("abd", "abd"));
    }

    @Test
    public void testBigDecimalComparator()
    {
        BigDecimal value = BigDecimal.valueOf(Long.MAX_VALUE).multiply(BigDecimal.valueOf(2));
        Decimal decimal1 = Decimal.apply(value);
        Decimal decimal2 = Decimal.apply(value.add(BigDecimal.ONE));
        assertTrue(createField(bridge.decimal()).compare(decimal1, decimal2) < 0);
        assertEquals(0, createField(bridge.decimal()).compare(decimal1, decimal1));
        assertEquals(0, createField(bridge.decimal()).compare(decimal2, decimal2));
        assertTrue(createField(bridge.decimal()).compare(decimal2, decimal1) > 0);
    }

    @Test
    public void testVarIntComparator()
    {
        BigDecimal value = BigDecimal.valueOf(Long.MAX_VALUE).multiply(BigDecimal.valueOf(2));
        Decimal decimal1 = Decimal.apply(value);
        Decimal decimal2 = Decimal.apply(value.add(BigDecimal.ONE));
        assertTrue(createField(bridge.varint()).compare(decimal1, decimal2) < 0);
        assertEquals(0, createField(bridge.varint()).compare(decimal1, decimal1));
        assertEquals(0, createField(bridge.varint()).compare(decimal2, decimal2));
        assertTrue(createField(bridge.varint()).compare(decimal2, decimal1) > 0);
    }

    @Test
    public void testIntegerComparator()
    {
        qt().forAll(integers().between(Integer.MIN_VALUE, Integer.MAX_VALUE - 1))
            .checkAssert(integer -> {
                assertTrue(createField(bridge.aInt()).compare(integer, integer + 1) < 0);
                assertEquals(0, createField(bridge.aInt()).compare(integer, integer));
                assertTrue(createField(bridge.aInt()).compare(integer + 1, integer) > 0);
            });
        assertEquals(0, createField(bridge.aInt()).compare(Integer.MAX_VALUE, Integer.MAX_VALUE));
        assertEquals(0, createField(bridge.aInt()).compare(Integer.MIN_VALUE, Integer.MIN_VALUE));
        assertTrue(createField(bridge.aInt()).compare(Integer.MIN_VALUE, Integer.MAX_VALUE) < 0);
        assertTrue(createField(bridge.aInt()).compare(Integer.MAX_VALUE, Integer.MIN_VALUE) > 0);
    }

    @Test
    public void testLongComparator()
    {
        assertTrue(createField(bridge.bigint()).compare(0L, 1L) < 0);
        assertEquals(0, createField(bridge.bigint()).compare(1L, 1L));
        assertTrue(createField(bridge.bigint()).compare(2L, 1L) > 0);
        assertEquals(0, createField(bridge.bigint()).compare(Long.MAX_VALUE, Long.MAX_VALUE));
        assertEquals(0, createField(bridge.bigint()).compare(Long.MIN_VALUE, Long.MIN_VALUE));
        assertTrue(createField(bridge.bigint()).compare(Long.MIN_VALUE, Long.MAX_VALUE) < 0);
        assertTrue(createField(bridge.bigint()).compare(Long.MAX_VALUE, Long.MIN_VALUE) > 0);
    }

    @Test
    public void testTimeComparator()
    {
        assertTrue(createField(bridge.time()).compare(0L, 1L) < 0);
        assertEquals(0, createField(bridge.time()).compare(1L, 1L));
        assertTrue(createField(bridge.time()).compare(2L, 1L) > 0);
        assertEquals(0, createField(bridge.time()).compare(Long.MAX_VALUE, Long.MAX_VALUE));
        assertEquals(0, createField(bridge.time()).compare(Long.MIN_VALUE, Long.MIN_VALUE));
        assertTrue(createField(bridge.time()).compare(Long.MIN_VALUE, Long.MAX_VALUE) < 0);
        assertTrue(createField(bridge.time()).compare(Long.MAX_VALUE, Long.MIN_VALUE) > 0);
    }

    @Test
    public void testBooleanComparator()
    {
        assertTrue(createField(bridge.bool()).compare(false, true) < 0);
        assertEquals(0, createField(bridge.bool()).compare(false, false));
        assertEquals(0, createField(bridge.bool()).compare(true, true));
        assertTrue(createField(bridge.bool()).compare(true, false) > 0);
    }

    @Test
    public void testFloatComparator()
    {
        assertTrue(createField(bridge.aFloat()).compare(1f, 2f) < 0);
        assertEquals(0, createField(bridge.aFloat()).compare(2f, 2f));
        assertTrue(createField(bridge.aFloat()).compare(2f, 1f) > 0);
    }

    @Test
    public void testDoubleComparator()
    {
        assertTrue(createField(bridge.aDouble()).compare(1d, 2d) < 0);
        assertEquals(0, createField(bridge.aDouble()).compare(2d, 2d));
        assertTrue(createField(bridge.aDouble()).compare(2d, 1d) > 0);
    }

    @Test
    public void testTimestampComparator()
    {
        long timestamp1 = 1L;
        long timestamp2 = 2L;
        assertTrue(createField(bridge.timestamp()).compare(timestamp1, timestamp2) < 0);
        assertEquals(0, createField(bridge.timestamp()).compare(timestamp1, timestamp1));
        assertEquals(0, createField(bridge.timestamp()).compare(timestamp2, timestamp2));
        assertTrue(createField(bridge.timestamp()).compare(timestamp2, timestamp1) > 0);
    }

    @Test
    public void testDateComparator()
    {
        int date1 = 1;
        int date2 = 2;
        assertTrue(createField(bridge.date()).compare(date1, date2) < 0);
        assertEquals(0, createField(bridge.date()).compare(date1, date1));
        assertEquals(0, createField(bridge.date()).compare(date2, date2));
        assertTrue(createField(bridge.date()).compare(date2, date1) > 0);
    }

    @Test
    public void testVoidComparator()
    {
        assertEquals(0, createField(bridge.empty()).compare(null, null));
    }

    @Test
    public void testShortComparator()
    {
        assertTrue(createField(bridge.smallint()).compare((short) 1, (short) 2) < 0);
        assertEquals(0, createField(bridge.smallint()).compare((short) 2, (short) 2));
        assertTrue(createField(bridge.smallint()).compare((short) 2, (short) 1) > 0);
    }

    @Test
    public void testByteArrayComparator()
    {
        byte[] bytes1 = new byte[]{0, 0, 0, 101};
        byte[] bytes2 = new byte[]{0, 0, 0, 102};
        byte[] bytes3 = new byte[]{0, 0, 1, 0};
        byte[] bytes4 = new byte[]{1, 0, 0, 0};
        assertTrue(createField(bridge.blob()).compare(bytes1, bytes2) < 0);
        assertEquals(0, createField(bridge.blob()).compare(bytes1, bytes1));
        assertEquals(0, createField(bridge.blob()).compare(bytes2, bytes2));
        assertTrue(createField(bridge.blob()).compare(bytes2, bytes1) > 0);
        assertTrue(createField(bridge.blob()).compare(bytes3, bytes1) > 0);
        assertTrue(createField(bridge.blob()).compare(bytes3, bytes2) > 0);
        assertTrue(createField(bridge.blob()).compare(bytes4, bytes3) > 0);
    }

    @Test
    public void testInetComparator() throws UnknownHostException
    {
        byte[] ip1 = InetAddress.getByAddress(CqlFieldComparators.toByteArray(2130706433)).getAddress();  // 127.0.0.1
        byte[] ip2 = InetAddress.getByAddress(CqlFieldComparators.toByteArray(2130706434)).getAddress();  // 127.0.0.2
        assertTrue(createField(bridge.inet()).compare(ip1, ip2) < 0);
        assertEquals(0, createField(bridge.inet()).compare(ip1, ip1));
        assertEquals(0, createField(bridge.inet()).compare(ip2, ip2));
        assertTrue(createField(bridge.inet()).compare(ip2, ip1) > 0);
    }

    private static byte[] toByteArray(int value)
    {
        return new byte[]{(byte) (value >> 24),
                          (byte) (value >> 16),
                          (byte) (value >>  8),
                          (byte)  value};
    }

    @Test
    public void testByteComparator()
    {
        byte byte1 = 101;
        byte byte2 = 102;
        assertTrue(createField(bridge.tinyint()).compare(byte1, byte2) < 0);
        assertEquals(0, createField(bridge.tinyint()).compare(byte1, byte1));
        assertEquals(0, createField(bridge.tinyint()).compare(byte2, byte2));
        assertTrue(createField(bridge.tinyint()).compare(byte2, byte1) > 0);
    }
}
