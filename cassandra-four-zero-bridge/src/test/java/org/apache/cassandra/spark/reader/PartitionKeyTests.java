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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridgeImplementation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.CqlType;
import org.apache.cassandra.spark.data.converter.SparkSqlTypeConverter;
import org.apache.cassandra.spark.utils.ComparisonUtils;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

public class PartitionKeyTests
{
    private static final CassandraBridgeImplementation BRIDGE = new CassandraBridgeImplementation();
    private static final SparkSqlTypeConverter TYPE_CONVERTER = TestSchema.getSparkSql();

    @Test
    @SuppressWarnings("static-access")
    public void testBuildPartitionKey()
    {
        qt().forAll(arbitrary().pick(BRIDGE.supportedTypes())).checkAssert(partitionKeyType -> {
            CqlTable table = TestSchema.builder(BRIDGE)
                                       .withPartitionKey("a", partitionKeyType)
                                       .withClusteringKey("b", BRIDGE.aInt())
                                       .withColumn("c", BRIDGE.aInt())
                                       .build()
                                       .buildTable();
            Object value = partitionKeyType.randomValue(100);
            String string = ((CqlType) partitionKeyType).serializer().toString(value);
            ByteBuffer buffer = BRIDGE.buildPartitionKey(table, Collections.singletonList(string));
            Object cassandraValue = partitionKeyType.deserializeToJavaType(buffer);

            // compare using Cassandra types
            assertTrue(ComparisonUtils.equals(value, cassandraValue));

            // convert SparkSQL types back into test row types to compare
            Object sparkSqlValue = TYPE_CONVERTER.convert(partitionKeyType, cassandraValue, false);
            assertTrue(ComparisonUtils.equals(value, TYPE_CONVERTER.toTestRowType(partitionKeyType, sparkSqlValue)));
        });
    }

    @Test
    @SuppressWarnings("static-access")
    public void testBuildCompositePartitionKey()
    {
        qt().forAll(arbitrary().pick(BRIDGE.supportedTypes())).checkAssert(partitionKeyType -> {
            CqlTable table = TestSchema.builder(BRIDGE)
                                       .withPartitionKey("a", BRIDGE.aInt())
                                       .withPartitionKey("b", partitionKeyType)
                                       .withPartitionKey("c", BRIDGE.text())
                                       .withClusteringKey("d", BRIDGE.aInt())
                                       .withColumn("e", BRIDGE.aInt())
                                       .build()
                                       .buildTable();
            List<AbstractType<?>> partitionKeyColumnTypes = BRIDGE.partitionKeyColumnTypes(table);
            CompositeType compositeType = CompositeType.getInstance(partitionKeyColumnTypes);

            int columnA = (int) BRIDGE.aInt().randomValue(1024);
            Object columnB = partitionKeyType.randomValue(1024);
            String columnBString = ((CqlType) partitionKeyType).serializer().toString(columnB);
            String columnC = (String) BRIDGE.text().randomValue(1024);

            ByteBuffer buffer = BRIDGE.buildPartitionKey(table, Arrays.asList(Integer.toString(columnA), columnBString, columnC));
            ByteBuffer[] buffers = compositeType.split(buffer);
            assertEquals(3, buffers.length);

            assertEquals(columnA, buffers[0].getInt());
            assertEquals(columnB, partitionKeyType.deserializeToJavaType(buffers[1]));
            assertEquals(columnC, TYPE_CONVERTER.toSparkType(BRIDGE.text()).toTestRowType(BRIDGE.text().deserializeToJavaType(buffers[2])));
        });
    }
}
