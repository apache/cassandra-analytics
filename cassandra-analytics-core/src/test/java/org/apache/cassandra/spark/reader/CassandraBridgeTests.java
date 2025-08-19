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
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class CassandraBridgeTests
{
    @Test
    public void testSparkDataTypes()
    {
        qt().forAll(TestUtils.bridges())
            .checkAssert(bridge -> {
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.timeuuid())).isEqualTo(DataTypes.StringType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.uuid())).isEqualTo(DataTypes.StringType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.ascii())).isEqualTo(DataTypes.StringType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.varchar())).isEqualTo(DataTypes.StringType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.text())).isEqualTo(DataTypes.StringType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.inet())).isEqualTo(DataTypes.BinaryType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.blob())).isEqualTo(DataTypes.BinaryType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.aInt())).isEqualTo(DataTypes.IntegerType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.date())).isEqualTo(DataTypes.DateType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.bigint())).isEqualTo(DataTypes.LongType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.time())).isEqualTo(DataTypes.LongType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.bool())).isEqualTo(DataTypes.BooleanType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.aFloat())).isEqualTo(DataTypes.FloatType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.aDouble())).isEqualTo(DataTypes.DoubleType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.timestamp())).isEqualTo(DataTypes.TimestampType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.empty())).isEqualTo(DataTypes.NullType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.smallint())).isEqualTo(DataTypes.ShortType);
                assertThat(getSparkSql(bridge).sparkSqlType(bridge.tinyint())).isEqualTo(DataTypes.ByteType);
            });
    }
}
