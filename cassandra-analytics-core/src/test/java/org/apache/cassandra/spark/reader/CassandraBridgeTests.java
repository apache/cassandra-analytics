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

package org.apache.cassandra.spark.reader;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.TestUtils;
import org.apache.spark.sql.types.DataTypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.quicktheories.QuickTheory.qt;

public class CassandraBridgeTests
{
    @Test
    public void testSparkDataTypes()
    {
        qt().forAll(TestUtils.bridges())
            .checkAssert(bridge -> {
                assertEquals(DataTypes.StringType, bridge.timeuuid().sparkSqlType());
                assertEquals(DataTypes.StringType, bridge.uuid().sparkSqlType());
                assertEquals(DataTypes.StringType, bridge.ascii().sparkSqlType());
                assertEquals(DataTypes.StringType, bridge.varchar().sparkSqlType());
                assertEquals(DataTypes.StringType, bridge.text().sparkSqlType());
                assertEquals(DataTypes.BinaryType, bridge.inet().sparkSqlType());
                assertEquals(DataTypes.BinaryType, bridge.blob().sparkSqlType());
                assertEquals(DataTypes.IntegerType, bridge.aInt().sparkSqlType());
                assertEquals(DataTypes.DateType, bridge.date().sparkSqlType());
                assertEquals(DataTypes.LongType, bridge.bigint().sparkSqlType());
                assertEquals(DataTypes.LongType, bridge.time().sparkSqlType());
                assertEquals(DataTypes.BooleanType, bridge.bool().sparkSqlType());
                assertEquals(DataTypes.FloatType, bridge.aFloat().sparkSqlType());
                assertEquals(DataTypes.DoubleType, bridge.aDouble().sparkSqlType());
                assertEquals(DataTypes.TimestampType, bridge.timestamp().sparkSqlType());
                assertEquals(DataTypes.NullType, bridge.empty().sparkSqlType());
                assertEquals(DataTypes.ShortType, bridge.smallint().sparkSqlType());
                assertEquals(DataTypes.ByteType, bridge.tinyint().sparkSqlType());
            });
    }
}
