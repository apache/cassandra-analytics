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

import static org.apache.cassandra.bridge.CassandraBridgeFactory.getSparkSql;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.quicktheories.QuickTheory.qt;

public class CassandraBridgeTests
{
    @Test
    public void testSparkDataTypes()
    {
        qt().forAll(TestUtils.bridges())
            .checkAssert(bridge -> {
                assertEquals(DataTypes.StringType, getSparkSql(bridge).sparkSqlType(bridge.timeuuid()));
                assertEquals(DataTypes.StringType, getSparkSql(bridge).sparkSqlType(bridge.uuid()));
                assertEquals(DataTypes.StringType, getSparkSql(bridge).sparkSqlType(bridge.ascii()));
                assertEquals(DataTypes.StringType, getSparkSql(bridge).sparkSqlType(bridge.varchar()));
                assertEquals(DataTypes.StringType, getSparkSql(bridge).sparkSqlType(bridge.text()));
                assertEquals(DataTypes.BinaryType, getSparkSql(bridge).sparkSqlType(bridge.inet()));
                assertEquals(DataTypes.BinaryType, getSparkSql(bridge).sparkSqlType(bridge.blob()));
                assertEquals(DataTypes.IntegerType, getSparkSql(bridge).sparkSqlType(bridge.aInt()));
                assertEquals(DataTypes.DateType, getSparkSql(bridge).sparkSqlType(bridge.date()));
                assertEquals(DataTypes.LongType, getSparkSql(bridge).sparkSqlType(bridge.bigint()));
                assertEquals(DataTypes.LongType, getSparkSql(bridge).sparkSqlType(bridge.time()));
                assertEquals(DataTypes.BooleanType, getSparkSql(bridge).sparkSqlType(bridge.bool()));
                assertEquals(DataTypes.FloatType, getSparkSql(bridge).sparkSqlType(bridge.aFloat()));
                assertEquals(DataTypes.DoubleType, getSparkSql(bridge).sparkSqlType(bridge.aDouble()));
                assertEquals(DataTypes.TimestampType, getSparkSql(bridge).sparkSqlType(bridge.timestamp()));
                assertEquals(DataTypes.NullType, getSparkSql(bridge).sparkSqlType(bridge.empty()));
                assertEquals(DataTypes.ShortType, getSparkSql(bridge).sparkSqlType(bridge.smallint()));
                assertEquals(DataTypes.ByteType, getSparkSql(bridge).sparkSqlType(bridge.tinyint()));
            });
    }
}
