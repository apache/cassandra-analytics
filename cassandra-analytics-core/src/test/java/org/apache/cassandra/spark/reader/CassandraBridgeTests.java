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

import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;

public class CassandraBridgeTests
{
    @Test
    public void testSparkDataTypes()
    {
        qt().forAll(TestUtils.bridges())
            .checkAssert(bridge -> {
                assertThat(bridge.getSchemaConverter().getDataType(bridge.timeuuid())).isEqualTo(DataTypes.StringType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.uuid())).isEqualTo(DataTypes.StringType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.ascii())).isEqualTo(DataTypes.StringType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.varchar())).isEqualTo(DataTypes.StringType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.text())).isEqualTo(DataTypes.StringType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.inet())).isEqualTo(DataTypes.BinaryType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.blob())).isEqualTo(DataTypes.BinaryType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.aInt())).isEqualTo(DataTypes.IntegerType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.date())).isEqualTo(DataTypes.DateType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.bigint())).isEqualTo(DataTypes.LongType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.time())).isEqualTo(DataTypes.LongType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.bool())).isEqualTo(DataTypes.BooleanType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.aFloat())).isEqualTo(DataTypes.FloatType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.aDouble())).isEqualTo(DataTypes.DoubleType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.timestamp())).isEqualTo(DataTypes.TimestampType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.empty())).isEqualTo(DataTypes.NullType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.smallint())).isEqualTo(DataTypes.ShortType);
                assertThat(bridge.getSchemaConverter().getDataType(bridge.tinyint())).isEqualTo(DataTypes.ByteType);
            });
    }
}
