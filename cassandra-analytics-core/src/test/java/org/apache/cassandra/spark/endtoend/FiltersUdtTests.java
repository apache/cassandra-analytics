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

package org.apache.cassandra.spark.endtoend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.test.TestSchema;

import static org.quicktheories.QuickTheory.qt;

@Tag("Sequential")
public class FiltersUdtTests
{
    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtNativeTypes(CassandraBridge bridge)
    {
        // pk -> a testudt<b text, c type, d int>
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.udt("keyspace", "testudt")
                                                                         .withField("b", bridge.text())
                                                                         .withField("c", type)
                                                                         .withField("d", bridge.aInt())
                                                                         .build()))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtInnerSet(CassandraBridge bridge)
    {
        // pk -> a testudt<b text, c frozen<type>, d int>
        qt().forAll(TestUtils.cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsSetElement)
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.udt("keyspace", "testudt")
                                                                         .withField("b", bridge.text())
                                                                         .withField("c", bridge.set(type).frozen())
                                                                         .withField("d", bridge.aInt())
                                                                         .build()))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtInnerList(CassandraBridge bridge)
    {
        // pk -> a testudt<b bigint, c frozen<list<type>>, d boolean>
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.udt("keyspace", "testudt")
                                                                         .withField("b", bridge.bigint())
                                                                         .withField("c", bridge.list(type).frozen())
                                                                         .withField("d", bridge.bool())
                                                                         .build()))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtInnerMap(CassandraBridge bridge)
    {
        // pk -> a testudt<b float, c frozen<set<uuid>>, d frozen<map<type1, type2>>, e boolean>
        qt().withExamples(50)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .assuming((type1, type2) -> type1.supportedAsMapKey())
            .checkAssert((type1, type2) ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.udt("keyspace", "testudt")
                                                                         .withField("b", bridge.aFloat())
                                                                         .withField("c", bridge.set(bridge.uuid()).frozen())
                                                                         .withField("d", bridge.map(type1, type2).frozen())
                                                                         .withField("e", bridge.bool())
                                                                         .build()))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMultipleUdts(CassandraBridge bridge)
    {
        // pk -> col1 udt1<a float, b frozen<set<uuid>>, c frozen<set<type>>, d boolean>,
        //       col2 udt2<a text, b bigint, g varchar>, col3 udt3<int, type, ascii>
        qt().forAll(TestUtils.cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsSetElement)
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("col1", bridge.udt("keyspace", "udt1")
                                                                            .withField("a", bridge.aFloat())
                                                                            .withField("b", bridge.set(bridge.uuid()).frozen())
                                                                            .withField("c", bridge.set(type).frozen())
                                                                            .withField("d", bridge.bool())
                                                                            .build())
                                                  .withColumn("col2", bridge.udt("keyspace", "udt2")
                                                                            .withField("a", bridge.text())
                                                                            .withField("b", bridge.bigint())
                                                                            .withField("g", bridge.varchar())
                                                                            .build())
                                                  .withColumn("col3", bridge.udt("keyspace", "udt3")
                                                                            .withField("a", bridge.aInt())
                                                                            .withField("b", bridge.list(type).frozen())
                                                                            .withField("c", bridge.ascii())
                                                                            .build()))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedUdt(CassandraBridge bridge)
    {
        // pk -> a test_udt<b float, c frozen<set<uuid>>, d frozen<nested_udt<x int, y type, z int>>, e boolean>
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.udt("keyspace", "test_udt")
                                                                         .withField("b", bridge.aFloat())
                                                                         .withField("c", bridge.set(bridge.uuid()).frozen())
                                                                         .withField("d", bridge.udt("keyspace", "nested_udt")
                                                                                               .withField("x", bridge.aInt())
                                                                                               .withField("y", type)
                                                                                               .withField("z", bridge.aInt())
                                                                                               .build().frozen())
                                                                         .withField("e", bridge.bool())
                                                                         .build()))
                               .run(bridge.getVersion())
            );
    }
}
