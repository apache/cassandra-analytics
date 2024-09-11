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
                assertEquals(DataTypes.StringType, bridge.typeConverter().sparkSqlType(bridge.timeuuid()));
                assertEquals(DataTypes.StringType, bridge.typeConverter().sparkSqlType(bridge.uuid()));
                assertEquals(DataTypes.StringType, bridge.typeConverter().sparkSqlType(bridge.ascii()));
                assertEquals(DataTypes.StringType, bridge.typeConverter().sparkSqlType(bridge.varchar()));
                assertEquals(DataTypes.StringType, bridge.typeConverter().sparkSqlType(bridge.text()));
                assertEquals(DataTypes.BinaryType, bridge.typeConverter().sparkSqlType(bridge.inet()));
                assertEquals(DataTypes.BinaryType, bridge.typeConverter().sparkSqlType(bridge.blob()));
                assertEquals(DataTypes.IntegerType, bridge.typeConverter().sparkSqlType(bridge.aInt()));
                assertEquals(DataTypes.DateType, bridge.typeConverter().sparkSqlType(bridge.date()));
                assertEquals(DataTypes.LongType, bridge.typeConverter().sparkSqlType(bridge.bigint()));
                assertEquals(DataTypes.LongType, bridge.typeConverter().sparkSqlType(bridge.time()));
                assertEquals(DataTypes.BooleanType, bridge.typeConverter().sparkSqlType(bridge.bool()));
                assertEquals(DataTypes.FloatType, bridge.typeConverter().sparkSqlType(bridge.aFloat()));
                assertEquals(DataTypes.DoubleType, bridge.typeConverter().sparkSqlType(bridge.aDouble()));
                assertEquals(DataTypes.TimestampType, bridge.typeConverter().sparkSqlType(bridge.timestamp()));
                assertEquals(DataTypes.NullType, bridge.typeConverter().sparkSqlType(bridge.empty()));
                assertEquals(DataTypes.ShortType, bridge.typeConverter().sparkSqlType(bridge.smallint()));
                assertEquals(DataTypes.ByteType, bridge.typeConverter().sparkSqlType(bridge.tinyint()));
            });
    }
}
