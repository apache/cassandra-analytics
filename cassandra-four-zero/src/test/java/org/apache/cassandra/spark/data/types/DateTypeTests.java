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

package org.apache.cassandra.spark.data.types;

import java.time.LocalDate;

import org.junit.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.serializers.SimpleDateSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DateTypeTests
{
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();

    @Test
    public void testDateConversion()
    {
        int cassandraDate = SimpleDateSerializer.dateStringToDays("2021-07-16");
        assertTrue(cassandraDate < 0);
        assertEquals("2021-07-16", SimpleDateSerializer.instance.toString(cassandraDate));
        Object sparkSqlDate = Date.INSTANCE.toSparkSqlType(cassandraDate, false);
        assertTrue(sparkSqlDate instanceof Integer);
        int numDays = (int) sparkSqlDate;
        assertTrue(numDays > 0);
        LocalDate end = LocalDate.of(1970, 1, 1)
                                       .plusDays(numDays);
        assertEquals(2021, end.getYear());
        assertEquals(7, end.getMonthValue());
        assertEquals(16, end.getDayOfMonth());
        Object cqlWriterObj = Date.INSTANCE.convertForCqlWriter(numDays, BRIDGE.getVersion());
        org.apache.cassandra.cql3.functions.types.LocalDate cqlWriterDate = (org.apache.cassandra.cql3.functions.types.LocalDate) cqlWriterObj;
        assertEquals(2021, cqlWriterDate.getYear());
        assertEquals(7, cqlWriterDate.getMonth());
        assertEquals(16, cqlWriterDate.getDay());
    }
}
