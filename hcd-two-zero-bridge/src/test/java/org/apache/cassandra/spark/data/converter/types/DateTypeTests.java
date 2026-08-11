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

package org.apache.cassandra.spark.data.converter.types;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.spark.data.types.Date;

import static org.assertj.core.api.Assertions.assertThat;

public class DateTypeTests
{
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();

    @Test
    public void testDateConversion()
    {
        int cassandraDate = SimpleDateSerializer.dateStringToDays("2021-07-16");
        assertThat(cassandraDate).isLessThan(0);
        assertThat(SimpleDateSerializer.instance.toString(cassandraDate)).isEqualTo("2021-07-16");
        Object sparkSqlDate = SparkDate.INSTANCE.toSparkSqlType(cassandraDate, false, false);
        assertThat(sparkSqlDate).isInstanceOf(Integer.class);
        int numDays = (int) sparkSqlDate;
        assertThat(numDays).isGreaterThan(0);
        LocalDate end = LocalDate.of(1970, 1, 1)
                                 .plusDays(numDays);
        assertThat(end.getYear()).isEqualTo(2021);
        assertThat(end.getMonthValue()).isEqualTo(7);
        assertThat(end.getDayOfMonth()).isEqualTo(16);
        Object cqlWriterObj = Date.INSTANCE.convertForCqlWriter(numDays, BRIDGE.getVersion(), false);
        org.apache.cassandra.cql3.functions.types.LocalDate cqlWriterDate = (org.apache.cassandra.cql3.functions.types.LocalDate) cqlWriterObj;
        assertThat(cqlWriterDate.getYear()).isEqualTo(2021);
        assertThat(cqlWriterDate.getMonth()).isEqualTo(7);
        assertThat(cqlWriterDate.getDay()).isEqualTo(16);
    }
}
