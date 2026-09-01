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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.TestUtils;
import org.apache.cassandra.spark.Tester;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.utils.test.TestSchema;
import org.apache.spark.sql.Row;
import org.quicktheories.core.Gen;
import scala.collection.mutable.AbstractSeq;

import static org.apache.cassandra.spark.utils.ScalaConversionUtils.mutableSeqAsJavaList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;

@Tag("Sequential")
public class DataTypeTests
{
    /* Data Type Tests */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testAllDataTypesPartitionKey(CassandraBridge bridge)
    {
        // Test partition key can be read for all data types
        qt().forAll(TestUtils.cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsPrimaryKeyColumn)
            .checkAssert(partitionKeyType -> {
                // Boolean or empty types have limited cardinality
                int numRows = partitionKeyType.cardinality(10);
                Tester.builder(TestSchema.builder(bridge)
                                         .withPartitionKey("a", partitionKeyType)
                                         .withColumn("b", bridge.bigint()))
                      .withNumRandomSSTables(1)
                      .withNumRandomRows(numRows)
                      .withExpectedRowCountPerSSTable(numRows)
                      .run(bridge.getVersion());
            });
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testAllDataTypesValueColumn(CassandraBridge bridge)
    {
        // Test value column can be read for all data types
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(valueType ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("a", bridge.bigint())
                                                  .withColumn("b", valueType))
                               .withNumRandomSSTables(1)
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run(bridge.getVersion())
            );
    }

    /* Collections */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSet(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsSetElement)
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.set(type)))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testVector(CassandraBridge bridge)
    {
        assumeThat(bridge.getVersion().versionNumber()).isGreaterThanOrEqualTo(CassandraVersion.FIVEZERO.versionNumber());
        qt().forAll(supportedVectorTypes(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.vector(type, 10)))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testVectorVector(CassandraBridge bridge)
    {
        assumeThat(bridge.getVersion().versionNumber()).isGreaterThanOrEqualTo(CassandraVersion.FIVEZERO.versionNumber());
        qt().forAll(supportedVectorTypes(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.vector(bridge.vector(type, 2), 5)))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testVectorList(CassandraBridge bridge)
    {
        assumeThat(bridge.getVersion().versionNumber()).isGreaterThanOrEqualTo(CassandraVersion.FIVEZERO.versionNumber());
        qt().forAll(supportedVectorTypes(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.vector(bridge.list(type), 3)))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testVectorUDT(CassandraBridge bridge)
    {
        // pk -> a vector<frozen<nested_udt<x int, y type, z int>>, 10>
        // Test vector of UDTs
        assumeThat(bridge.getVersion().versionNumber()).isGreaterThanOrEqualTo(CassandraVersion.FIVEZERO.versionNumber());
        qt().withExamples(10)
            .forAll(supportedVectorTypes(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.vector(
                                                                                bridge.udt("keyspace", "nested_udt")
                                                                                      .withField("x", bridge.aInt())
                                                                                      .withField("y", type)
                                                                                      .withField("z", bridge.aInt())
                                                                                      .build().frozen(),
                                                                                10)))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testVectorTuple(CassandraBridge bridge)
    {
        // pk -> a vector<frozen<tuple<type, float, text>>, 7>
        // Test tuple nested within vector
        assumeThat(bridge.getVersion().versionNumber()).isGreaterThanOrEqualTo(CassandraVersion.FIVEZERO.versionNumber());
        qt().withExamples(10)
            .forAll(supportedVectorTypes(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.vector(bridge.tuple(type,
                                                                                              bridge.aFloat(),
                                                                                              bridge.text()).frozen(), 7)))
                               .run(bridge.getVersion())
            );
    }

    private static Gen<CqlField.NativeType> supportedVectorTypes(CassandraBridge bridge)
    {
        // TODO: Vector of list of durations fail, because we cannot replace DurationSerializer with
        //  AnalyticsDurationSerializer across all serializers used by VectorType.
        List<CqlField.NativeType> supportedTypes = bridge.supportedTypes().stream()
                                                         .filter(t -> !t.equals(bridge.duration()))
                                                         .collect(Collectors.toList());
        return arbitrary().pick(supportedTypes);
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testList(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.list(type)))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMap(CassandraBridge bridge)
    {
        qt().withExamples(50)  // Limit number of tests otherwise n x n tests takes too long
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .assuming((keyType, valueType) -> keyType.supportedAsMapKey())
            .checkAssert((keyType, valueType) ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.map(keyType, valueType)))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testClusteringKeySet(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withClusteringKey("id", bridge.aInt())
                                 .withColumn("a", bridge.set(bridge.text())))
              .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
              .run(bridge.getVersion());
    }

    /* Frozen Collections */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testFrozenSet(CassandraBridge bridge)
    {
        // pk -> a frozen<set<?>>
        qt().forAll(TestUtils.cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsSetElement)
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.set(type).frozen()))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testFrozenList(CassandraBridge bridge)
    {
        // pk -> a frozen<list<?>>
        qt().forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.list(type).frozen()))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testFrozenMap(CassandraBridge bridge)
    {
        // pk -> a frozen<map<?, ?>>
        qt().withExamples(50)  // Limit number of tests otherwise n x n tests takes too long
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .assuming((keyType, valueType) -> keyType.supportedAsMapKey())
            .checkAssert((keyType, valueType) ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.map(keyType, valueType).frozen()))
                               .withExpectedRowCountPerSSTable(Tester.DEFAULT_NUM_ROWS)
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedMapSet(CassandraBridge bridge)
    {
        // pk -> a map<text, frozen<set<text>>>
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("a", bridge.map(bridge.text(), bridge.set(bridge.text()).frozen())))
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedMapList(CassandraBridge bridge)
    {
        // pk -> a map<text, frozen<list<text>>>
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("a", bridge.map(bridge.text(), bridge.list(bridge.text()).frozen())))
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedMapMap(CassandraBridge bridge)
    {
        // pk -> a map<text, frozen<map<bigint, varchar>>>
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("a", bridge.map(bridge.text(),
                                                             bridge.map(bridge.bigint(), bridge.varchar()).frozen())))
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .dontCheckNumSSTables()
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testFrozenNestedMapMap(CassandraBridge bridge)
    {
        // pk -> a frozen<map<text, <map<int, timestamp>>>
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("a", bridge.map(bridge.text(),
                                                             bridge.map(bridge.aInt(), bridge.timestamp())).frozen()))
              .withNumRandomRows(32)
              .withExpectedRowCountPerSSTable(32)
              .dontCheckNumSSTables()
              .run(bridge.getVersion());
    }

    /* Tuples */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testBasicTuple(CassandraBridge bridge)
    {
        // pk -> a tuple<int, type1, bigint, type2>
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.tuple(bridge.aInt(), type1, bridge.bigint(), type2)))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleWithClusteringKey(CassandraBridge bridge)
    {
        // pk -> col1 type1 -> a tuple<int, type2, bigint>
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .assuming((type1, type2) -> type1.supportedAsPrimaryKeyColumn())
            .checkAssert((type1, type2) ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withClusteringKey("col1", type1)
                                                  .withColumn("a", bridge.tuple(bridge.aInt(), type2, bridge.bigint())))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedTuples(CassandraBridge bridge)
    {
        // pk -> a tuple<varchar, tuple<int, type1, float, varchar, tuple<bigint, boolean, type2>>, timeuuid>
        // Test tuples nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .checkAssert((type1, type2) ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.tuple(bridge.varchar(),
                                                                                bridge.tuple(bridge.aInt(),
                                                                                             type1,
                                                                                             bridge.aFloat(),
                                                                                             bridge.varchar(),
                                                                                             bridge.tuple(bridge.bigint(),
                                                                                                          bridge.bool(),
                                                                                                          type2)),
                                                                                bridge.timeuuid())))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleSet(CassandraBridge bridge)
    {
        // pk -> a tuple<varchar, tuple<int, varchar, float, varchar, set<type>>, timeuuid>
        // Test set nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsSetElement)
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.tuple(bridge.varchar(),
                                                                                bridge.tuple(bridge.aInt(),
                                                                                             bridge.varchar(),
                                                                                             bridge.aFloat(),
                                                                                             bridge.varchar(),
                                                                                             bridge.set(type)),
                                                                                bridge.timeuuid())))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleList(CassandraBridge bridge)
    {
        // pk -> a tuple<varchar, tuple<int, varchar, float, varchar, list<type>>, timeuuid>
        // Test list nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.tuple(bridge.varchar(),
                                                                                bridge.tuple(bridge.aInt(),
                                                                                             bridge.varchar(),
                                                                                             bridge.aFloat(),
                                                                                             bridge.varchar(),
                                                                                             bridge.list(type)),
                                                                                bridge.timeuuid())))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleMap(CassandraBridge bridge)
    {
        // pk -> a tuple<varchar, tuple<int, varchar, float, varchar, map<type1, type2>>, timeuuid>
        // Test map nested within tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge), TestUtils.cql3Type(bridge))
            .assuming((type1, type2) -> type1.supportedAsMapKey())
            .checkAssert((type1, type2) ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.tuple(bridge.varchar(),
                                                                                bridge.tuple(bridge.aInt(),
                                                                                             bridge.varchar(),
                                                                                             bridge.aFloat(),
                                                                                             bridge.varchar(),
                                                                                             bridge.map(type1, type2)),
                                                                                bridge.timeuuid())))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testMapTuple(CassandraBridge bridge)
    {
        // pk -> a map<timeuuid, frozen<tuple<boolean, type, timestamp>>>
        // Test tuple nested within map
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.map(bridge.timeuuid(),
                                                                              bridge.tuple(bridge.bool(),
                                                                                           type,
                                                                                           bridge.timestamp()).frozen())))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testSetTuple(CassandraBridge bridge)
    {
        // pk -> a set<frozen<tuple<type, float, text>>>
        // Test tuple nested within set
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsSetElement)
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.set(bridge.tuple(type,
                                                                                           bridge.aFloat(),
                                                                                           bridge.text()).frozen())))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testListTuple(CassandraBridge bridge)
    {
        // pk -> a list<frozen<tuple<int, inet, decimal, type>>>
        // Test tuple nested within map
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.list(bridge.tuple(bridge.aInt(),
                                                                                            bridge.inet(),
                                                                                            bridge.decimal(),
                                                                                            type).frozen())))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleUDT(CassandraBridge bridge)
    {
        // pk -> a tuple<varchar, frozen<nested_udt<x int, y type, z int>>, timeuuid>
        // Test tuple with inner UDT
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.tuple(bridge.varchar(),
                                                                                bridge.udt("keyspace", "nested_udt")
                                                                                      .withField("x", bridge.aInt())
                                                                                      .withField("y", type)
                                                                                      .withField("z", bridge.aInt())
                                                                                      .build().frozen(),
                                                                                bridge.timeuuid())))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUDTTuple(CassandraBridge bridge)
    {
        // pk -> a nested_udt<x text, y tuple<int, float, type, timestamp>, z ascii>
        // Test UDT with inner tuple
        qt().withExamples(10)
            .forAll(TestUtils.cql3Type(bridge))
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withColumn("a", bridge.udt("keyspace", "nested_udt")
                                                                         .withField("x", bridge.text())
                                                                         .withField("y", bridge.tuple(bridge.aInt(),
                                                                                                      bridge.aFloat(),
                                                                                                      type,
                                                                                                      bridge.timestamp()))
                                                                         .withField("z", bridge.ascii())
                                                                         .build()))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testTupleClusteringKey(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsPrimaryKeyColumn)
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withClusteringKey("ck", bridge.tuple(bridge.aInt(),
                                                                                        bridge.text(),
                                                                                        type,
                                                                                        bridge.aFloat()))
                                                  .withColumn("a", bridge.text())
                                                  .withColumn("b", bridge.aInt())
                                                  .withColumn("c", bridge.ascii()))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtClusteringKey(CassandraBridge bridge)
    {
        qt().forAll(TestUtils.cql3Type(bridge))
            .assuming(CqlField.CqlType::supportedAsPrimaryKeyColumn)
            .checkAssert(type ->
                         Tester.builder(TestSchema.builder(bridge)
                                                  .withPartitionKey("pk", bridge.uuid())
                                                  .withClusteringKey("ck", bridge.udt("keyspace", "udt1")
                                                                                 .withField("a", bridge.text())
                                                                                 .withField("b", type)
                                                                                 .withField("c", bridge.aInt())
                                                                                 .build().frozen())
                                                  .withColumn("a", bridge.text())
                                                  .withColumn("b", bridge.aInt())
                                                  .withColumn("c", bridge.ascii()))
                               .run(bridge.getVersion())
            );
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testComplexSchema(CassandraBridge bridge)
    {
        String keyspace = "complex_schema2";
        CqlField.CqlUdt udt1 = bridge.udt(keyspace, "udt1")
                                     .withField("time", bridge.bigint())
                                     .withField("time_offset_minutes", bridge.aInt())
                                     .build();
        CqlField.CqlUdt udt2 = bridge.udt(keyspace, "udt2")
                                     .withField("version", bridge.text())
                                     .withField("id", bridge.text())
                                     .withField("platform", bridge.text())
                                     .withField("time_range", bridge.text())
                                     .build();
        CqlField.CqlUdt udt3 = bridge.udt(keyspace, "udt3")
                                     .withField("field", bridge.text())
                                     .withField("time_with_zone", udt1)
                                     .build();
        CqlField.CqlUdt udt4 = bridge.udt(keyspace, "udt4")
                                     .withField("first_seen", udt3.frozen())
                                     .withField("last_seen", udt3.frozen())
                                     .withField("first_transaction", udt3.frozen())
                                     .withField("last_transaction", udt3.frozen())
                                     .withField("first_listening", udt3.frozen())
                                     .withField("last_listening", udt3.frozen())
                                     .withField("first_reading", udt3.frozen())
                                     .withField("last_reading", udt3.frozen())
                                     .withField("output_event", bridge.text())
                                     .withField("event_history", bridge.map(bridge.bigint(),
                                                                            bridge.map(bridge.text(),
                                                                                       bridge.bool()).frozen()
                                     ).frozen())
                                     .build();

        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace(keyspace1)
                                              .withPartitionKey("\"consumerId\"", bridge.text())
                                              .withClusteringKey("dimensions", udt2.frozen())
                                              .withColumn("fields", udt4.frozen())
                                              .withColumn("first_transition_time", udt1.frozen())
                                              .withColumn("last_transition_time", udt1.frozen())
                                              .withColumn("prev_state_id", bridge.text())
                                              .withColumn("state_id", bridge.text()))
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testNestedFrozenUDT(CassandraBridge bridge)
    {
        // "(a bigint PRIMARY KEY, b <map<int, frozen<testudt>>>)"
        // testudt(a text, b bigint, c int)
        CqlField.CqlUdt testudt = bridge.udt("nested_frozen_udt", "testudt")
                                        .withField("a", bridge.text())
                                        .withField("b", bridge.bigint())
                                        .withField("c", bridge.aInt())
                                        .build();
        Tester.builder(keyspace -> TestSchema.builder(bridge)
                                             .withKeyspace(keyspace)
                                             .withPartitionKey("a", bridge.bigint())
                                             .withColumn("b", bridge.map(bridge.aInt(), testudt.frozen())))
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testDeepNestedUDT(CassandraBridge bridge)
    {
        String keyspace = "deep_nested_frozen_udt";
        CqlField.CqlUdt udt1 = bridge.udt(keyspace, "udt1")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.aInt())
                                     .withField("c", bridge.bigint())
                                     .build();
        CqlField.CqlUdt udt2 = bridge.udt(keyspace, "udt2")
                                     .withField("a", bridge.aInt())
                                     .withField("b", bridge.set(bridge.uuid()))
                                     .build();
        CqlField.CqlUdt udt3 = bridge.udt(keyspace, "udt3")
                                     .withField("a", bridge.aInt())
                                     .withField("b", bridge.set(bridge.uuid()))
                                     .build();
        CqlField.CqlUdt udt4 = bridge.udt(keyspace, "udt4")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.text())
                                     .withField("c", bridge.uuid())
                                     .withField("d", bridge.list(bridge.tuple(bridge.text(),
                                                                              bridge.bigint()).frozen()
                                     ).frozen())
                                     .build();
        CqlField.CqlUdt udt5 = bridge.udt(keyspace, "udt5")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.text())
                                     .withField("c", bridge.bigint())
                                     .withField("d", bridge.set(udt4.frozen()))
                                     .build();
        CqlField.CqlUdt udt6 = bridge.udt(keyspace, "udt6")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.text())
                                     .withField("c", bridge.aInt())
                                     .withField("d", bridge.aInt())
                                     .build();
        CqlField.CqlUdt udt7 = bridge.udt(keyspace, "udt7")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.uuid())
                                     .withField("c", bridge.bool())
                                     .withField("d", bridge.bool())
                                     .withField("e", bridge.bool())
                                     .withField("f", bridge.bigint())
                                     .withField("g", bridge.bigint())
                                     .build();

        CqlField.CqlUdt udt8 = bridge.udt(keyspace, "udt8")
                                     .withField("a", bridge.text())
                                     .withField("b", bridge.bool())
                                     .withField("c", bridge.bool())
                                     .withField("d", bridge.bool())
                                     .withField("e", bridge.bigint())
                                     .withField("f", bridge.bigint())
                                     .withField("g", bridge.uuid())
                                     .withField("h", bridge.bigint())
                                     .withField("i", bridge.uuid())
                                     .withField("j", bridge.uuid())
                                     .withField("k", bridge.uuid())
                                     .withField("l", bridge.uuid())
                                     .withField("m", bridge.aInt())
                                     .withField("n", bridge.timestamp())
                                     .withField("o", bridge.text())
                                     .build();

        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace(keyspace1)
                                              .withPartitionKey("pk", bridge.uuid())
                                              .withClusteringKey("ck", bridge.uuid())
                                              .withColumn("a", udt3.frozen())
                                              .withColumn("b", udt2.frozen())
                                              .withColumn("c", bridge.set(bridge.tuple(udt1,
                                                                                       bridge.text()).frozen()))
                                              .withColumn("d", bridge.set(bridge.tuple(bridge.bigint(),
                                                                                       bridge.text()).frozen()))
                                              .withColumn("e", bridge.set(bridge.tuple(udt2,
                                                                                       bridge.text()).frozen()))
                                              .withColumn("f", bridge.set(udt7.frozen()))
                                              .withColumn("g", bridge.map(bridge.aInt(),
                                                                          bridge.set(bridge.text()).frozen()))
                                              .withColumn("h", bridge.set(bridge.tinyint()))
                                              .withColumn("i", bridge.map(bridge.text(),
                                                                          udt6.frozen()))
                                              .withColumn("j", bridge.map(bridge.text(),
                                                                          bridge.map(bridge.text(),
                                                                                     bridge.text()).frozen()))
                                              .withColumn("k", bridge.list(bridge.tuple(bridge.text(),
                                                                                        bridge.text(),
                                                                                        bridge.text()).frozen()))
                                              .withColumn("l", bridge.list(udt5.frozen()))
                                              .withColumn("m", udt8.frozen())
                                              .withMinCollectionSize(4))
              .withNumRandomRows(50)
              .withNumRandomSSTables(2)
              .run(bridge.getVersion());
    }

    /* BigDecimal/Integer Tests */

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testBigDecimal(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("c1", bridge.decimal())
                                 .withColumn("c2", bridge.text()))
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testBigInteger(CassandraBridge bridge)
    {
        Tester.builder(TestSchema.builder(bridge)
                                 .withPartitionKey("pk", bridge.uuid())
                                 .withColumn("c1", bridge.varint())
                                 .withColumn("c2", bridge.text()))
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    public void testUdtFieldOrdering(CassandraBridge bridge)
    {
        String keyspace = "udt_field_ordering";
        CqlField.CqlUdt udt1 = bridge.udt(keyspace, "udt1")
                                     .withField("c", bridge.text())
                                     .withField("b", bridge.uuid())
                                     .withField("a", bridge.bool())
                                     .build();
        Tester.builder(keyspace1 -> TestSchema.builder(bridge)
                                              .withKeyspace(keyspace1)
                                              .withPartitionKey("pk", bridge.uuid())
                                              .withColumn("a", bridge.set(udt1.frozen())))
              .run(bridge.getVersion());
    }

    @ParameterizedTest
    @MethodSource("org.apache.cassandra.bridge.VersionRunner#bridges")
    @SuppressWarnings("unchecked")
    public void testUdtTupleInnerNulls(CassandraBridge bridge)
    {
        CqlField.CqlUdt udtType = bridge.udt("udt_inner_nulls", "udt")
                                        .withField("a", bridge.uuid())
                                        .withField("b", bridge.text())
                                        .build();
        CqlField.CqlTuple tupleType = bridge.tuple(bridge.bigint(), bridge.text(), bridge.aInt());

        int numRows = 50;
        int midPoint = numRows / 2;
        Random random = new Random();
        Map<UUID, Set<Map<String, Object>>> udtSetValues = new LinkedHashMap<>(numRows);
        Map<UUID, Object[]> tupleValues = new LinkedHashMap<>(numRows);
        for (int tupleIndex = 0; tupleIndex < numRows; tupleIndex++)
        {
            UUID pk = UUID.randomUUID();
            Set<Map<String, Object>> udtSet = new HashSet<>(numRows);
            for (int udtIndex = 0; udtIndex < numRows; udtIndex++)
            {
                Map<String, Object> udt = Maps.newHashMapWithExpectedSize(2);
                udt.put("a", UUID.randomUUID());
                udt.put("b", udtIndex < midPoint ? UUID.randomUUID().toString() : null);
                udtSet.add(udt);
            }
            Object[] tuple = new Object[]{random.nextLong(),
                                          tupleIndex < midPoint ? UUID.randomUUID().toString() : null,
                                          random.nextInt()};

            udtSetValues.put(pk, udtSet);
            tupleValues.put(pk, tuple);
        }

        Tester.builder(keyspace -> TestSchema.builder(bridge)
                                             .withKeyspace(keyspace)
                                             .withPartitionKey("pk", bridge.uuid())
                                             .withColumn("a", bridge.set(udtType.frozen()))
                                             .withColumn("b", tupleType))
              .dontWriteRandomData()
              .withSSTableWriter(writer -> {
                  for (UUID pk : udtSetValues.keySet())
                  {
                      Set<Object> udtSet = udtSetValues.get(pk).stream()
                                                       .map(map ->  bridge.toUserTypeValue(udtType, map))
                                                       .collect(Collectors.toSet());
                      Object tuple = bridge.toTupleValue(tupleType, tupleValues.get(pk));

                      writer.write(pk, udtSet, tuple);
                  }
              })
              .withCheck(dataset -> {
                  for (Row row : dataset.collectAsList())
                  {
                      UUID pk = UUID.fromString(row.getString(0));

                      Set<Map<String, Object>> expectedUdtSet = udtSetValues.get(pk);
                      List<Row> udtSet = mutableSeqAsJavaList((AbstractSeq<Row>) row.get(1));
                      assertThat(udtSet.size()).isEqualTo(expectedUdtSet.size());
                      for (Row udt : udtSet)
                      {
                          Map<String, Object> expectedUdt = Maps.newHashMapWithExpectedSize(2);
                          expectedUdt.put("a", UUID.fromString(udt.getString(0)));
                          expectedUdt.put("b", udt.getString(1));
                          assertThat(expectedUdtSet.contains(expectedUdt)).isTrue();
                      }

                      Object[] expectedTuple = tupleValues.get(pk);
                      Row tuple = (Row) row.get(2);
                      assertThat(tuple.length()).isEqualTo(expectedTuple.length);
                      assertThat(tuple.getLong(0)).isEqualTo(expectedTuple[0]);
                      assertThat(tuple.getString(1)).isEqualTo(expectedTuple[1]);
                      assertThat(tuple.getInt(2)).isEqualTo(expectedTuple[2]);
                  }
              })
              .run(bridge.getVersion());
    }
}
