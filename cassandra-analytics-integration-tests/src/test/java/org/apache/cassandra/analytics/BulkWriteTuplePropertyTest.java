/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.quicktheories.core.Gen;

import static org.apache.cassandra.analytics.BulkWriteTuplePropertyTest.DeeplyNestedTupleData.*;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;
import static org.quicktheories.generators.SourceDSL.maps;
import static org.quicktheories.generators.SourceDSL.strings;

/**
 * Property-based testing using QuickTheories for complex tuple types.
 * Uses qt() to generate random test data and validate round-trip write/read.
 *
 * <p>This test enhances standard property-based testing with comprehensive null handling:
 * <ul>
 *   <li><b>Random nulls:</b> Generators include ~20% null values at the top-level data structure</li>
 *   <li><b>Guaranteed nulls:</b> Each batch includes at least 2 null rows for explicit null testing</li>
 *   <li><b>Missing columns:</b> Null rows simulate missing column values (column not set)</li>
 *   <li><b>Partial nulls:</b> Complex nested structures test nulls at various nesting levels</li>
 * </ul>
 *
 * <p>This approach combines the benefits of:
 * <ul>
 *   <li>Property-based testing: Discovers unexpected edge cases with random data</li>
 *   <li>Example-based testing: Explicitly tests known edge cases like nulls</li>
 * </ul>
 */
class BulkWriteTuplePropertyTest extends SharedClusterSparkIntegrationTestBase
{
    // Number of rows to test per test method
    private static final int NUM_ROWS = 50;

    // Probability of null values (20% chance of null)
    private static final double NULL_PROBABILITY = 0.2;

    private static final QualifiedName TUPLE_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuples");
    private static final QualifiedName LIST_OF_TUPLES_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_list_tuples");
    private static final QualifiedName SET_OF_TUPLES_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_set_tuples");
    private static final QualifiedName MAP_WITH_TUPLES_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_map_tuples");
    private static final QualifiedName MAP_WITH_TUPLE_KEY_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_map_tuple_key");
    private static final QualifiedName NESTED_TUPLE_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_nested_tuples");
    private static final QualifiedName UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_udt");
    private static final QualifiedName TUPLE_WITH_LIST_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_list");
    private static final QualifiedName TUPLE_WITH_SET_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_set");
    private static final QualifiedName TUPLE_WITH_MAP_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_map");
    private static final QualifiedName TUPLE_WITH_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_udt");
    private static final QualifiedName TUPLE_WITH_LIST_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_list_udt");
    private static final QualifiedName TUPLE_WITH_SET_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_set_udt");
    private static final QualifiedName TUPLE_WITH_MAP_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_map_udt");
    private static final QualifiedName MULTI_TUPLE_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_multi_tuple");
    private static final QualifiedName DEEPLY_NESTED_TUPLE_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_deeply_nested");
    private static final QualifiedName TUPLE_ALL_COLLECTIONS_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_all_coll");
    private static final QualifiedName MAP_TUPLE_KEY_VALUE_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_map_tuple_kv");
    private static final QualifiedName TUPLE_SET_OF_TUPLES_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_set_tuples");
    private static final QualifiedName TUPLE_NESTED_COLLECTIONS_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_nested_coll");
    private static final QualifiedName TUPLE_LIST_OF_TUPLES_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_list_tuples");

    /**
     * Tests simple tuple schema: frozen&lt;tuple&lt;int, text&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuples (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, text&gt;&gt;)
     * <p>Tests: Basic two-field tuple with primitive types, including null tuples
     */
    @Test
    void testSimpleTwoFieldTupleIntText()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleData> tupleData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleDataBatchGen())
            .check(batch -> {
                tupleData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createDataFrame(spark, tupleData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_TABLE).save();
        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = String.format("Row %d mismatch\nSource: %s\nRead:   %s",
                i, formatTupleRow(sourceRow), formatTupleRow(readRow));

            assertThat(readRow.getLong(0))
                .as(context)
                .isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1))
                    .as(context)
                    .isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getInt(0))
                    .as(context)
                    .isEqualTo(sourceTuple.getInt(0));
                assertThat(readTuple.getString(1))
                    .as(context)
                    .isEqualTo(sourceTuple.getString(1));
            }
        }

        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests list collection containing tuples: list&lt;frozen&lt;tuple&lt;int, text&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_list_tuples (id BIGINT PRIMARY KEY, data list&lt;frozen&lt;tuple&lt;int, text&gt;&gt;&gt;)
     * <p>Tests: List of tuples with variable length, null lists, and null tuples within lists
     */
    @Test
    void testListCollectionOfTuples()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<ListOfTuplesData> data = new ArrayList<>();

        qt().withExamples(1)
            .forAll(listOfTuplesBatchGen())
            .check(batch -> {
                data.addAll(batch);
                return true;
            });


        Dataset<Row> sourceData = createListOfTuplesDataFrame(spark, data);
        bulkWriterDataFrameWriter(sourceData, LIST_OF_TUPLES_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(LIST_OF_TUPLES_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));



            assertThat(readRow.getLong(0))
                .as(context)
                .isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1))
                    .as(context)
                    .isTrue();
            }
            else
            {
                List<Row> sourceList = sourceRow.getList(1);
                List<Row> readList = readRow.getList(1);

                assertThat(readList)
                    .as(context)
                    .hasSize(sourceList.size());
                for (int j = 0; j < sourceList.size(); j++)
                {
                    Row sourceTuple = sourceList.get(j);
                    Row readTuple = readList.get(j);
                    String tupleContext = context + String.format("\n  Tuple[%d]: source=%s, read=%s",
                        j, formatSimpleTuple(sourceTuple), formatSimpleTuple(readTuple));
                    assertThat(readTuple.getInt(0))
                        .as(tupleContext)
                        .isEqualTo(sourceTuple.getInt(0));
                    assertThat(readTuple.getString(1))
                        .as(tupleContext)
                        .isEqualTo(sourceTuple.getString(1));
                }
            }
        }
        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests map with tuple values: map&lt;text, frozen&lt;tuple&lt;int, text&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_map_tuples (id BIGINT PRIMARY KEY, data map&lt;text, frozen&lt;tuple&lt;int, text&gt;&gt;&gt;)
     * <p>Tests: Map with string keys and tuple values, null maps, variable map sizes
     */
    @Test
    void testMapWithTupleValues()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<MapWithTuplesData> data = new ArrayList<>();

        qt().withExamples(1)
            .forAll(mapWithTuplesBatchGen())
            .check(batch -> {
                data.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createMapWithTuplesDataFrame(spark, data);
        bulkWriterDataFrameWriter(sourceData, MAP_WITH_TUPLES_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(MAP_WITH_TUPLES_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));



            assertThat(readRow.getLong(0))
                .as(context)
                .isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1))
                    .as(context)
                    .isTrue();
            }
            else
            {
                Map<String, Row> sourceMap = sourceRow.getJavaMap(1);
                Map<String, Row> readMap = readRow.getJavaMap(1);

                assertThat(readMap)
                    .as(context)
                    .hasSize(sourceMap.size());
                for (String key : sourceMap.keySet())
                {
                    assertThat(readMap)
                        .as(context + String.format("\n  Missing key: '%s'", key))
                        .containsKey(key);
                    Row sourceTuple = sourceMap.get(key);
                    Row readTuple = readMap.get(key);
                    String tupleContext = context + String.format("\n  Key['%s']: source=%s, read=%s",
                        key, formatSimpleTuple(sourceTuple), formatSimpleTuple(readTuple));
                    assertThat(readTuple.getInt(0))
                        .as(tupleContext)
                        .isEqualTo(sourceTuple.getInt(0));
                    assertThat(readTuple.getString(1))
                        .as(tupleContext)
                        .isEqualTo(sourceTuple.getString(1));
                }
            }
        }

        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests nested tuples (2 levels): frozen&lt;tuple&lt;int, frozen&lt;tuple&lt;text, int&gt;&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_nested_tuples (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, frozen&lt;tuple&lt;text, int&gt;&gt;&gt;&gt;)
     * <p>Tests: Tuple containing another tuple, null at both nesting levels
     */
    @Test
    void testNestedTuplesWithTwoLevels()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<NestedTupleData> data = new ArrayList<>();

        qt().withExamples(1)
            .forAll(nestedTuplesBatchGen())
            .check(batch -> {
                data.addAll(batch);
                return true;
            });


        Dataset<Row> sourceData = createNestedTuplesDataFrame(spark, data);
        bulkWriterDataFrameWriter(sourceData, NESTED_TUPLE_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(NESTED_TUPLE_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));



            assertThat(readRow.getLong(0))
                .as(context)
                .isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1))
                    .as(context)
                    .isTrue();
            }
            else
            {
                Row sourceOuter = sourceRow.getStruct(1);
                Row readOuter = readRow.getStruct(1);

                assertThat(readOuter.getInt(0))
                    .as(context)
                    .isEqualTo(sourceOuter.getInt(0));

                Row sourceInner = sourceOuter.getStruct(1);
                Row readInner = readOuter.getStruct(1);

                assertThat(readInner.getString(0))
                    .as(context)
                    .isEqualTo(sourceInner.getString(0));
                assertThat(readInner.getInt(1))
                    .as(context)
                    .isEqualTo(sourceInner.getInt(1));
            }
        }
        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests UDT containing a tuple field: frozen&lt;person&gt; where person has frozen&lt;tuple&lt;int, text&gt;&gt; address
     * <p>Table: CREATE TABLE qt_udt (id BIGINT PRIMARY KEY, data frozen&lt;person&gt;)
     * <p>UDT: CREATE TYPE person (name text, age int, address frozen&lt;tuple&lt;int, text&gt;&gt;)
     * <p>Tests: User-defined type with tuple field, null UDT, null tuple within UDT
     */
    @Test
    void testUdtWithTupleField()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<UdtData> data = new ArrayList<>();

        qt().withExamples(1)
            .forAll(udtBatchGen())
            .check(batch -> {
                data.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createUdtDataFrame(spark, data);
        bulkWriterDataFrameWriter(sourceData, UDT_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(UDT_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));



            assertThat(readRow.getLong(0))
                .as(context)
                .isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1))
                    .as(context)
                    .isTrue();
            }
            else
            {
                Row sourceUdt = sourceRow.getStruct(1);
                Row readUdt = readRow.getStruct(1);

                assertThat(readUdt.getString(0))
                    .as(context)
                    .isEqualTo(sourceUdt.getString(0));
                assertThat(readUdt.getInt(1))
                    .as(context)
                    .isEqualTo(sourceUdt.getInt(1));

                // nested tuple inside UDT
                Row sourceTuple = sourceUdt.getStruct(2);
                Row readTuple = readUdt.getStruct(2);
                assertThat(readTuple.getInt(0))
                    .as(context)
                    .isEqualTo(sourceTuple.getInt(0));
                assertThat(readTuple.getString(1))
                    .as(context)
                    .isEqualTo(sourceTuple.getString(1));
            }
        }

        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests set collection containing tuples: set&lt;frozen&lt;tuple&lt;int, text&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_set_tuples (id BIGINT PRIMARY KEY, data set&lt;frozen&lt;tuple&lt;int, text&gt;&gt;&gt;)
     * <p>Tests: Set of tuples (unordered, unique), null sets, deduplication of identical tuples
     */
    @Test
    void testSetCollectionOfTuples()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<SetOfTuplesData> setOfTuplesData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(setOfTuplesBatchGen())
            .check(batch -> {
                setOfTuplesData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createSetOfTuplesDataFrame(spark, setOfTuplesData);
        bulkWriterDataFrameWriter(sourceData, SET_OF_TUPLES_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(SET_OF_TUPLES_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));



            assertThat(readRow.getLong(0))
                .as(context)
                .isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1))
                    .as(context)
                    .isTrue();
            }
            else
            {
                List<Row> sourceSet = sourceRow.getList(1);
                List<Row> readSet = readRow.getList(1);

                assertThat(readSet)
                    .as(context)
                    .hasSize(sourceSet.size());
                // Note: sets can be in different order, so we need to compare contents
                Set<String> sourceStrings = new HashSet<>();
                Set<String> readStrings = new HashSet<>();
                for (Row r : sourceSet)
                {
                    sourceStrings.add(r.getInt(0) + ":" + r.getString(1));
                }
                for (Row r : readSet)
                {
                    readStrings.add(r.getInt(0) + ":" + r.getString(1));
                }
                assertThat(readStrings)
                    .as(context)
                    .isEqualTo(sourceStrings);
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests map with tuple keys: map&lt;frozen&lt;tuple&lt;int, text&gt;&gt;, text&gt;
     * <p>Table: CREATE TABLE qt_map_tuple_key (id BIGINT PRIMARY KEY, data map&lt;frozen&lt;tuple&lt;int, text&gt;&gt;, text&gt;)
     * <p>Tests: Using tuples as map keys, null maps, tuple key comparison and hashing
     */
    @Test
    void testMapWithTupleKeys()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<MapWithTupleKeyData> mapWithTupleKeyData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(mapWithTupleKeyBatchGen())
            .check(batch -> {
                mapWithTupleKeyData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createMapWithTupleKeyDataFrame(spark, mapWithTupleKeyData);
        bulkWriterDataFrameWriter(sourceData, MAP_WITH_TUPLE_KEY_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(MAP_WITH_TUPLE_KEY_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));



            assertThat(readRow.getLong(0))
                .as(context)
                .isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1))
                    .as(context)
                    .isTrue();
            }
            else
            {
                Map<Row, String> sourceMap = sourceRow.getJavaMap(1);
                Map<Row, String> readMap = readRow.getJavaMap(1);

                assertThat(readMap)
                    .as(context)
                    .hasSize(sourceMap.size());
                // Compare by converting to string representation
                Map<String, String> sourceStringMap = new HashMap<>();
                Map<String, String> readStringMap = new HashMap<>();
                for (Map.Entry<Row, String> entry : sourceMap.entrySet())
                {
                    sourceStringMap.put(entry.getKey().getInt(0) + ":" + entry.getKey().getString(1), entry.getValue());
                }
                for (Map.Entry<Row, String> entry : readMap.entrySet())
                {
                    readStringMap.put(entry.getKey().getInt(0) + ":" + entry.getKey().getString(1), entry.getValue());
                }
                assertThat(readStringMap)
                    .as(context)
                    .isEqualTo(sourceStringMap);
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests tuple containing a list: frozen&lt;tuple&lt;int, list&lt;text&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuple_list (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, list&lt;text&gt;&gt;&gt;)
     * <p>Tests: Tuple with collection field, null tuples, empty vs null lists within tuples
     */
    @Test
    void testTupleContainingList()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleWithListData> tupleWithListData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleWithListBatchGen())
            .check(batch -> {
                tupleWithListData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createTupleWithListDataFrame(spark, tupleWithListData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_WITH_LIST_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_WITH_LIST_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));



            assertThat(readRow.getLong(0))
                .as(context)
                .isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1))
                    .as(context)
                    .isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getInt(0))
                    .as(context)
                    .isEqualTo(sourceTuple.getInt(0));
                assertThat(readTuple.getList(1))
                    .as(context)
                    .isEqualTo(sourceTuple.getList(1));
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests tuple containing a set: frozen&lt;tuple&lt;int, set&lt;int&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuple_set (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, set&lt;int&gt;&gt;&gt;)
     * <p>Tests: Tuple with set field, null tuples, empty vs null sets, set ordering
     */
    @Test
    void testTupleContainingSet()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleWithSetData> tupleWithSetData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleWithSetBatchGen())
            .check(batch -> {
                tupleWithSetData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createTupleWithSetDataFrame(spark, tupleWithSetData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_WITH_SET_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_WITH_SET_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));



            assertThat(readRow.getLong(0))
                .as(context)
                .isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1))
                    .as(context)
                    .isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getInt(0))
                    .as(context)
                    .isEqualTo(sourceTuple.getInt(0));
                // Sets may be in different order, so compare as sets
                Set<Integer> sourceSet = new HashSet<>(sourceTuple.getList(1));
                Set<Integer> readSet = new HashSet<>(readTuple.getList(1));
                assertThat(readSet)
                    .as(context)
                    .isEqualTo(sourceSet);
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests tuple containing a map: frozen&lt;tuple&lt;int, map&lt;text, int&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuple_map (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, map&lt;text, int&gt;&gt;&gt;)
     * <p>Tests: Tuple with map field, null tuples, empty vs null maps, map key-value pairs
     */
    @Test
    void testTupleContainingMap()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleWithMapData> tupleWithMapData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleWithMapBatchGen())
            .check(batch -> {
                tupleWithMapData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createTupleWithMapDataFrame(spark, tupleWithMapData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_WITH_MAP_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_WITH_MAP_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));



            assertThat(readRow.getLong(0))
                .as(context)
                .isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1))
                    .as(context)
                    .isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getInt(0))
                    .as(context)
                    .isEqualTo(sourceTuple.getInt(0));
                assertThat(readTuple.getJavaMap(1))
                    .as(context)
                    .isEqualTo(sourceTuple.getJavaMap(1));
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests tuple containing UDT with collections: frozen&lt;tuple&lt;int, frozen&lt;udt_with_collections&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuple_udt (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, frozen&lt;udt_with_collections&gt;&gt;&gt;)
     * <p>UDT: CREATE TYPE udt_with_collections (f1 list&lt;text&gt;, f2 set&lt;text&gt;, f3 map&lt;int, text&gt;, f4 tuple&lt;int, text&gt;)
     * <p>Tests: Complex nesting - tuple containing UDT containing collections and nested tuple
     */
    @Test
    void testTupleContainingUdtWithCollections()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleWithUdtData> tupleWithUdtData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleWithUdtBatchGen())
            .check(batch -> {
                tupleWithUdtData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createTupleWithUdtDataFrame(spark, tupleWithUdtData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_WITH_UDT_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_WITH_UDT_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));


            assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1)).as(context).isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getInt(0)).as(context).isEqualTo(sourceTuple.getInt(0));

                Row sourceUdt = sourceTuple.getStruct(1);
                Row readUdt = readTuple.getStruct(1);

                assertThat(readUdt.getList(0)).as(context).isEqualTo(sourceUdt.getList(0));
                assertThat(new HashSet<>(readUdt.getList(1))).as(context).isEqualTo(new HashSet<>(sourceUdt.getList(1)));
                assertThat(readUdt.getJavaMap(2)).as(context).isEqualTo(sourceUdt.getJavaMap(2));

                Row sourceTupleInUdt = sourceUdt.getStruct(3);
                Row readTupleInUdt = readUdt.getStruct(3);
                assertThat(readTupleInUdt.getInt(0)).as(context).isEqualTo(sourceTupleInUdt.getInt(0));
                assertThat(readTupleInUdt.getString(1)).as(context).isEqualTo(sourceTupleInUdt.getString(1));
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests tuple containing list of UDTs: frozen&lt;tuple&lt;int, list&lt;frozen&lt;simple_udt&gt;&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuple_list_udt (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, list&lt;frozen&lt;simple_udt&gt;&gt;&gt;&gt;)
     * <p>UDT: CREATE TYPE simple_udt (field1 int, field2 text)
     * <p>Tests: Tuple with collection of UDTs, null lists, variable list sizes
     */
    @Test
    void testTupleContainingListOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleWithListUdtData> tupleWithListUdtData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleWithListUdtBatchGen())
            .check(batch -> {
                tupleWithListUdtData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createTupleWithListUdtDataFrame(spark, tupleWithListUdtData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_WITH_LIST_UDT_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_WITH_LIST_UDT_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));

            

            assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1)).as(context).isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getInt(0)).as(context).isEqualTo(sourceTuple.getInt(0));

                List<Row> sourceList = sourceTuple.getList(1);
                List<Row> readList = readTuple.getList(1);
                assertThat(readList).as(context)
                .hasSize(sourceList.size());

                for (int j = 0; j < sourceList.size(); j++)
                {
                    assertThat(readList.get(j).getInt(0))
                        .as(context)
                        .isEqualTo(sourceList.get(j).getInt(0));
                    assertThat(readList.get(j).getString(1))
                        .as(context)
                        .isEqualTo(sourceList.get(j).getString(1));
                }
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests tuple containing set of UDTs: frozen&lt;tuple&lt;int, set&lt;frozen&lt;simple_udt&gt;&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuple_set_udt (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, set&lt;frozen&lt;simple_udt&gt;&gt;&gt;&gt;)
     * <p>UDT: CREATE TYPE simple_udt (field1 int, field2 text)
     * <p>Tests: Tuple with set of UDTs, null sets, UDT equality and deduplication
     */
    @Test
    void testTupleContainingSetOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleWithSetUdtData> tupleWithSetUdtData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleWithSetUdtBatchGen())
            .check(batch -> {
                tupleWithSetUdtData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createTupleWithSetUdtDataFrame(spark, tupleWithSetUdtData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_WITH_SET_UDT_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_WITH_SET_UDT_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));

            

            assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1)).as(context).isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getInt(0)).as(context).isEqualTo(sourceTuple.getInt(0));

                List<Row> sourceList = sourceTuple.getList(1);
                List<Row> readList = readTuple.getList(1);
                assertThat(readList).as(context)
                .hasSize(sourceList.size());

                // Sets can be in different order
                Set<String> sourceSet = sourceList.stream()
                    .map(r -> r.getInt(0) + ":" + r.getString(1))
                    .collect(Collectors.toSet());
                Set<String> readSet = readList.stream()
                    .map(r -> r.getInt(0) + ":" + r.getString(1))
                    .collect(Collectors.toSet());
                assertThat(readSet).as(context)
                .isEqualTo(sourceSet);
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests tuple containing map with UDT values: frozen&lt;tuple&lt;int, map&lt;text, frozen&lt;simple_udt&gt;&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuple_map_udt (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, map&lt;text, frozen&lt;simple_udt&gt;&gt;&gt;&gt;)
     * <p>UDT: CREATE TYPE simple_udt (field1 int, field2 text)
     * <p>Tests: Tuple with map of UDTs, null maps, UDT as map values
     */
    @Test
    void testTupleContainingMapOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleWithMapUdtData> tupleWithMapUdtData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleWithMapUdtBatchGen())
            .check(batch -> {
                tupleWithMapUdtData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createTupleWithMapUdtDataFrame(spark, tupleWithMapUdtData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_WITH_MAP_UDT_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_WITH_MAP_UDT_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));

            

            assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1)).as(context).isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getInt(0)).as(context).isEqualTo(sourceTuple.getInt(0));

                Map<String, Row> sourceMap = sourceTuple.getJavaMap(1);
                Map<String, Row> readMap = readTuple.getJavaMap(1);
                assertThat(readMap).as(context)
                .hasSize(sourceMap.size());

                for (String key : sourceMap.keySet())
                {
                    assertThat(readMap).as(context)
                .containsKey(key);
                    assertThat(readMap.get(key).getInt(0))
                        .as(context)
                        .isEqualTo(sourceMap.get(key).getInt(0));
                    assertThat(readMap.get(key).getString(1))
                        .as(context)
                        .isEqualTo(sourceMap.get(key).getString(1));
                }
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests table with multiple tuple columns of different types
     * <p>Table: CREATE TABLE qt_multi_tuple (id BIGINT PRIMARY KEY,
     *          tuple1 frozen&lt;tuple&lt;int, text&gt;&gt;,
     *          tuple2 frozen&lt;tuple&lt;text, int, bigint&gt;&gt;,
     *          tuple3 frozen&lt;tuple&lt;list&lt;text&gt;, set&lt;int&gt;&gt;&gt;)
     * <p>Tests: Multiple tuple columns in same table, varying arities (2, 3 fields), mixed types
     */
    @Test
    void testMultipleTupleColumnsWithDifferentTypes()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<MultiTupleData> multiTupleData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(multiTupleBatchGen())
            .check(batch -> {
                multiTupleData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createMultiTupleDataFrame(spark, multiTupleData);
        bulkWriterDataFrameWriter(sourceData, MULTI_TUPLE_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(MULTI_TUPLE_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));


            assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

            // Check tuple1
            Row sourceTuple1 = sourceRow.getStruct(1);
            Row readTuple1 = readRow.getStruct(1);
            assertThat(readTuple1.getInt(0)).as(context).isEqualTo(sourceTuple1.getInt(0));
            assertThat(readTuple1.getString(1)).as(context).isEqualTo(sourceTuple1.getString(1));

            // Check tuple2
            Row sourceTuple2 = sourceRow.getStruct(2);
            Row readTuple2 = readRow.getStruct(2);
            assertThat(readTuple2.getString(0)).as(context).isEqualTo(sourceTuple2.getString(0));
            assertThat(readTuple2.getInt(1)).as(context).isEqualTo(sourceTuple2.getInt(1));
            assertThat(readTuple2.getLong(2)).as(context).isEqualTo(sourceTuple2.getLong(2));

            // Check tuple3
            Row sourceTuple3 = sourceRow.getStruct(3);
            Row readTuple3 = readRow.getStruct(3);
            assertThat(readTuple3.getList(0)).as(context).isEqualTo(sourceTuple3.getList(0));
            assertThat(new HashSet<>(readTuple3.getList(1))).as(context).isEqualTo(new HashSet<>(sourceTuple3.getList(1)));
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests deeply nested tuples (3 levels): frozen&lt;tuple&lt;int, frozen&lt;tuple&lt;text, frozen&lt;tuple&lt;bigint, text&gt;&gt;&gt;&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_deeply_nested (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, frozen&lt;tuple&lt;text, frozen&lt;tuple&lt;bigint, text&gt;&gt;&gt;&gt;&gt;&gt;)
     * <p>Tests: Maximum nesting depth, null propagation through nesting levels
     */
    @Test
    void testDeeplyNestedTuplesThreeLevels()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<DeeplyNestedTupleData> deeplyNestedTupleData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(deeplyNestedTupleBatchGen())
            .check(batch -> {
                deeplyNestedTupleData.addAll(batch);
                return true;
            });


        Dataset<Row> sourceData = createDeeplyNestedTupleDataFrame(spark, deeplyNestedTupleData);
        bulkWriterDataFrameWriter(sourceData, DEEPLY_NESTED_TUPLE_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(DEEPLY_NESTED_TUPLE_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));


            assertThat(readRow.getLong(0))
                .as(context)
                .isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1))
                    .as(context)
                    .isTrue();
            }
            else
            {
                Row sourceLevel1 = sourceRow.getStruct(1);
                Row readLevel1 = readRow.getStruct(1);
                assertThat(readLevel1.getInt(0))
                    .as(context)
                    .isEqualTo(sourceLevel1.getInt(0));

                Row sourceLevel2 = sourceLevel1.getStruct(1);
                Row readLevel2 = readLevel1.getStruct(1);
                assertThat(readLevel2.getString(0))
                    .as(context)
                    .isEqualTo(sourceLevel2.getString(0));

                Row sourceLevel3 = sourceLevel2.getStruct(1);
                Row readLevel3 = readLevel2.getStruct(1);
                assertThat(readLevel3.getLong(0))
                    .as(context)
                    .isEqualTo(sourceLevel3.getLong(0));
                assertThat(readLevel3.getString(1))
                    .as(context)
                    .isEqualTo(sourceLevel3.getString(1));
            }
        }
        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests tuple containing all collection types: frozen&lt;tuple&lt;list&lt;text&gt;, set&lt;int&gt;, map&lt;text, int&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuple_all_coll (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;list&lt;text&gt;, set&lt;int&gt;, map&lt;text, int&gt;&gt;&gt;)
     * <p>Tests: Single tuple with all three collection types, null collections, empty vs null
     */
    @Test
    void testTupleContainingAllCollectionTypes()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleAllCollectionsData> tupleAllCollectionsData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleAllCollectionsBatchGen())
            .check(batch -> {
                tupleAllCollectionsData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createTupleAllCollectionsDataFrame(spark, tupleAllCollectionsData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_ALL_COLLECTIONS_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_ALL_COLLECTIONS_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));


            assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1)).as(context).isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getList(0)).as(context).isEqualTo(sourceTuple.getList(0));
                assertThat(new HashSet<>(readTuple.getList(1))).as(context).isEqualTo(new HashSet<>(sourceTuple.getList(1)));
                assertThat(readTuple.getJavaMap(2)).as(context).isEqualTo(sourceTuple.getJavaMap(2));
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests map with both tuple keys and tuple values: map&lt;frozen&lt;tuple&lt;int, text&gt;&gt;, frozen&lt;tuple&lt;text, int&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_map_tuple_kv (id BIGINT PRIMARY KEY, data map&lt;frozen&lt;tuple&lt;int, text&gt;&gt;, frozen&lt;tuple&lt;text, int&gt;&gt;&gt;)
     * <p>Tests: Tuples as both map keys and values, tuple comparison for keys, null handling
     */
    @Test
    void testMapWithTupleKeysAndTupleValues()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<MapTupleKeyValueData> mapTupleKeyValueData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(mapTupleKeyValueBatchGen())
            .check(batch -> {
                mapTupleKeyValueData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createMapTupleKeyValueDataFrame(spark, mapTupleKeyValueData);
        bulkWriterDataFrameWriter(sourceData, MAP_TUPLE_KEY_VALUE_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(MAP_TUPLE_KEY_VALUE_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));

            

            assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1)).as(context).isTrue();
            }
            else
            {
                Map<Row, Row> sourceMap = sourceRow.getJavaMap(1);
                Map<Row, Row> readMap = readRow.getJavaMap(1);
                assertThat(readMap).as(context)
                .hasSize(sourceMap.size());

                // Compare using string representation
                Map<String, String> sourceStringMap = new HashMap<>();
                Map<String, String> readStringMap = new HashMap<>();
                for (Map.Entry<Row, Row> entry : sourceMap.entrySet())
                {
                    String key = entry.getKey().getInt(0) + ":" + entry.getKey().getString(1);
                    String value = entry.getValue().getString(0) + ":" + entry.getValue().getInt(1);
                    sourceStringMap.put(key, value);
                }
                for (Map.Entry<Row, Row> entry : readMap.entrySet())
                {
                    String key = entry.getKey().getInt(0) + ":" + entry.getKey().getString(1);
                    String value = entry.getValue().getString(0) + ":" + entry.getValue().getInt(1);
                    readStringMap.put(key, value);
                }
                assertThat(readStringMap).as(context)
                .isEqualTo(sourceStringMap);
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests tuple containing set of tuples: frozen&lt;tuple&lt;int, set&lt;frozen&lt;tuple&lt;text, int&gt;&gt;&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuple_set_tuples (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, set&lt;frozen&lt;tuple&lt;text, int&gt;&gt;&gt;&gt;&gt;)
     * <p>Tests: Tuple containing collection of tuples, nested tuple deduplication in set
     */
    @Test
    void testTupleContainingSetOfTuples()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleSetOfTuplesData> tupleSetOfTuplesData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleSetOfTuplesBatchGen())
            .check(batch -> {
                tupleSetOfTuplesData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createTupleSetOfTuplesDataFrame(spark, tupleSetOfTuplesData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_SET_OF_TUPLES_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_SET_OF_TUPLES_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));

            

            assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1)).as(context).isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getInt(0)).as(context).isEqualTo(sourceTuple.getInt(0));

                List<Row> sourceSet = sourceTuple.getList(1);
                List<Row> readSet = readTuple.getList(1);
                assertThat(readSet).as(context)
                .hasSize(sourceSet.size());

                Set<String> sourceStrings = sourceSet.stream()
                    .map(r -> r.getString(0) + ":" + r.getInt(1))
                    .collect(Collectors.toSet());
                Set<String> readStrings = readSet.stream()
                    .map(r -> r.getString(0) + ":" + r.getInt(1))
                    .collect(Collectors.toSet());
                assertThat(readStrings).as(context)
                .isEqualTo(sourceStrings);
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests tuple with nested collections and nested tuple: frozen&lt;tuple&lt;int, list&lt;int&gt;, frozen&lt;tuple&lt;text, set&lt;int&gt;&gt;&gt;, map&lt;text, text&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuple_nested_coll (id BIGINT PRIMARY KEY,
     *          data frozen&lt;tuple&lt;int, list&lt;int&gt;, frozen&lt;tuple&lt;text, set&lt;int&gt;&gt;&gt;, map&lt;text, text&gt;&gt;&gt;)
     * <p>Tests: Complex tuple with multiple collections and nested tuple, mixed collection types
     */
    @Test
    void testTupleWithNestedCollectionsAndNestedTuple()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleNestedCollectionsData> tupleNestedCollectionsData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleNestedCollectionsBatchGen())
            .check(batch -> {
                tupleNestedCollectionsData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createTupleNestedCollectionsDataFrame(spark, tupleNestedCollectionsData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_NESTED_COLLECTIONS_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_NESTED_COLLECTIONS_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));


            assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1)).as(context).isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getInt(0)).as(context).isEqualTo(sourceTuple.getInt(0));
                assertThat(readTuple.getList(1)).as(context).isEqualTo(sourceTuple.getList(1));

                Row sourceNestedTuple = sourceTuple.getStruct(2);
                Row readNestedTuple = readTuple.getStruct(2);
                assertThat(readNestedTuple.getString(0)).as(context).isEqualTo(sourceNestedTuple.getString(0));
                assertThat(new HashSet<>(readNestedTuple.getList(1))).as(context).isEqualTo(new HashSet<>(sourceNestedTuple.getList(1)));

                assertThat(readTuple.getJavaMap(3)).as(context).isEqualTo(sourceTuple.getJavaMap(3));
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    /**
     * Tests tuple containing list of tuples: frozen&lt;tuple&lt;int, list&lt;frozen&lt;tuple&lt;text, set&lt;int&gt;&gt;&gt;&gt;&gt;&gt;
     * <p>Table: CREATE TABLE qt_tuple_list_tuples (id BIGINT PRIMARY KEY,
     *          data frozen&lt;tuple&lt;int, list&lt;frozen&lt;tuple&lt;text, set&lt;int&gt;&gt;&gt;&gt;&gt;&gt;)
     * <p>Tests: Tuple containing list of nested tuples, each nested tuple contains a set
     */
    @Test
    void testTupleContainingListOfTuples()
    {
        SparkSession spark = getOrCreateSparkSession();

        List<TupleListOfTuplesData> tupleListOfTuplesData = new ArrayList<>();

        qt().withExamples(1)
            .forAll(tupleListOfTuplesBatchGen())
            .check(batch -> {
                tupleListOfTuplesData.addAll(batch);
                return true;
            });

        Dataset<Row> sourceData = createTupleListOfTuplesDataFrame(spark, tupleListOfTuplesData);
        bulkWriterDataFrameWriter(sourceData, TUPLE_LIST_OF_TUPLES_TABLE).save();

        Dataset<Row> readData = bulkReaderDataFrame(TUPLE_LIST_OF_TUPLES_TABLE).load();

        List<Row> sourceRows = sourceData.sort("id").collectAsList();
        List<Row> readRows = readData.sort("id").collectAsList();

        assertThat(readRows)
                .hasSize(sourceRows.size());

        for (int i = 0; i < sourceRows.size(); i++)
        {
            Row sourceRow = sourceRows.get(i);
            Row readRow = readRows.get(i);

            String context = formatContext(i, formatGenericRow(sourceRow), formatGenericRow(readRow));

            

            assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

            if (sourceRow.isNullAt(1))
            {
                assertThat(readRow.isNullAt(1)).as(context).isTrue();
            }
            else
            {
                Row sourceTuple = sourceRow.getStruct(1);
                Row readTuple = readRow.getStruct(1);

                assertThat(readTuple.getInt(0)).as(context).isEqualTo(sourceTuple.getInt(0));

                List<Row> sourceList = sourceTuple.getList(1);
                List<Row> readList = readTuple.getList(1);
                assertThat(readList).as(context)
                .hasSize(sourceList.size());

                for (int j = 0; j < sourceList.size(); j++)
                {
                    Row sourceNestedTuple = sourceList.get(j);
                    Row readNestedTuple = readList.get(j);
                    assertThat(readNestedTuple.getString(0)).as(context).isEqualTo(sourceNestedTuple.getString(0));
                    assertThat(new HashSet<>(readNestedTuple.getList(1))).as(context).isEqualTo(new HashSet<>(sourceNestedTuple.getList(1)));
                }
            }
        }


        sourceData.unpersist();
        readData.unpersist();
    }

    private String sanitizeTypeName(String typeName)
    {
        // Convert type name to valid table name (remove <, >, spaces, etc.)
        return typeName.replaceAll("[<>,\\s]", "_")
                      .replaceAll("__+", "_")
                      .toLowerCase();
    }

    /**
     * Helper to create a nullable version of any generator.
     * Returns null with NULL_PROBABILITY chance.
     */
    private <T> Gen<T> nullable(Gen<T> gen)
    {
        return integers().between(0, 100)
            .flatMap(i -> {
                if (i < (NULL_PROBABILITY * 100))
                {
                    // Generate a null by mapping any value to null
                    return integers().between(0, 1).map(x -> null);
                }
                else
                {
                    return gen;
                }
            });
    }

    /**
     * Helper to create a generator that includes explicit null test cases.
     * Ensures at least some nulls are tested by adding explicit null values.
     */
    private <T> Gen<List<T>> batchGenWithNulls(Gen<T> itemGen)
    {
        // Generate most items normally, but ensure some nulls
        return lists().of(nullable(itemGen)).ofSize(BulkWriteTuplePropertyTest.NUM_ROWS)
            .map(list -> {
                // Ensure at least 2 nulls in the batch for testing
                if (list.stream().filter(Objects::isNull).count() < 2)
                {
                    List<T> newList = new ArrayList<>(list);
                    if (!newList.isEmpty()) newList.set(0, null);
                    if (newList.size() > 1) newList.set(newList.size() - 1, null);
                    return newList;
                }
                return list;
            });
    }

    private Gen<List<TupleData>> tupleDataBatchGen()
    {
        Gen<TupleData> tuplegen = integers().between(0, 1000)
            .zip(strings().allPossible().ofLengthBetween(1, 20),
                 TupleData::new);
        return batchGenWithNulls(tuplegen);
    }

    private Gen<List<ListOfTuplesData>> listOfTuplesBatchGen()
    {
        Gen<TupleData> tupleGen = integers().between(0, 500)
            .zip(strings().allPossible().ofLengthBetween(1, 15),
                 TupleData::new);

        Gen<ListOfTuplesData> rowGen = lists().of(tupleGen)
            .ofSizeBetween(1, 5)
            .map(ListOfTuplesData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<MapWithTuplesData>> mapWithTuplesBatchGen()
    {
        Gen<TupleData> tupleGen = integers().between(0, 500)
            .zip(strings().allPossible().ofLengthBetween(1, 15),
                 TupleData::new);

        Gen<Map<String, TupleData>> mapGen = lists().of(
                strings().allPossible().ofLengthBetween(1, 10)
                    .zip(tupleGen, AbstractMap.SimpleEntry::new))
            .ofSizeBetween(1, 5)
            .map(entries -> entries.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b)));

        Gen<MapWithTuplesData> rowGen = mapGen.map(MapWithTuplesData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<NestedTupleData>> nestedTuplesBatchGen()
    {
        Gen<NestedTupleData.InnerTuple> innerTupleGen = strings().allPossible().ofLengthBetween(1, 15)
            .zip(integers().between(0, 500), NestedTupleData.InnerTuple::new);

        Gen<NestedTupleData> rowGen = integers().between(0, 1000)
            .zip(innerTupleGen, NestedTupleData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<UdtData>> udtBatchGen()
    {
        Gen<TupleData> tupleGen = integers().between(0, 500)
            .zip(strings().allPossible().ofLengthBetween(1, 15),
                 TupleData::new);

        Gen<UdtData> udtGen = strings().allPossible().ofLengthBetween(1, 20)
            .zip(integers().between(18, 100), tupleGen,
                 UdtData::new);

        return batchGenWithNulls(udtGen);
    }

    private Gen<List<SetOfTuplesData>> setOfTuplesBatchGen()
    {
        Gen<TupleData> tupleGen = integers().between(0, 500)
            .zip(strings().allPossible().ofLengthBetween(1, 15),
                 TupleData::new);

        Gen<SetOfTuplesData> rowGen = lists().of(tupleGen)
            .ofSizeBetween(1, 5)
            .map(list -> new SetOfTuplesData(new HashSet<>(list)));

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<MapWithTupleKeyData>> mapWithTupleKeyBatchGen()
    {
        Gen<TupleData> keyGen = integers().between(0, 100)
            .zip(strings().allPossible().ofLengthBetween(1, 10),
                 TupleData::new);

        Gen<Map<TupleData, String>> mapGen = lists().of(
                keyGen.zip(strings().allPossible().ofLengthBetween(1, 15),
                           AbstractMap.SimpleEntry::new))
            .ofSizeBetween(1, 5)
            .map(entries -> entries.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b)));

        Gen<MapWithTupleKeyData> rowGen = mapGen.map(MapWithTupleKeyData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<TupleWithListData>> tupleWithListBatchGen()
    {
        Gen<TupleWithListData> rowGen = integers().between(0, 1000)
            .zip(lists().of(strings().allPossible().ofLengthBetween(1, 15)).ofSizeBetween(1, 5),
                 TupleWithListData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<TupleWithSetData>> tupleWithSetBatchGen()
    {
        Gen<List<Integer>> setGen = lists().of(integers().between(0, 100))
            .ofSizeBetween(1, 5);

        Gen<TupleWithSetData> rowGen = integers().between(0, 1000)
            .zip(setGen, (i, list) -> new TupleWithSetData(i, new HashSet<>(list)));

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<TupleWithMapData>> tupleWithMapBatchGen()
    {
        Gen<Map<String, Integer>> mapGen = maps().of(
            strings().allPossible().ofLengthBetween(1, 10),
            integers().between(0, 1000)
        ).ofSizeBetween(1, 4);

        Gen<TupleWithMapData> rowGen = integers().between(0, 1000)
            .zip(mapGen, TupleWithMapData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<TupleWithUdtData>> tupleWithUdtBatchGen()
    {
        Gen<TupleData> tupleGen = integers().between(0, 500)
            .zip(strings().allPossible().ofLengthBetween(1, 15),
                 TupleData::new);

        Gen<UdtWithCollectionsData> udtGen = lists().of(strings().allPossible().ofLengthBetween(1, 10))
            .ofSizeBetween(1, 3)
            .zip(lists().of(strings().allPossible().ofLengthBetween(1, 10)).ofSizeBetween(1, 3),
                 maps().of(integers().between(0, 100), strings().allPossible().ofLengthBetween(1, 10)).ofSizeBetween(1, 3),
                 tupleGen,
                 UdtWithCollectionsData::new);

        Gen<TupleWithUdtData> rowGen = integers().between(0, 1000)
            .zip(udtGen, TupleWithUdtData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<TupleWithListUdtData>> tupleWithListUdtBatchGen()
    {
        Gen<SimpleUdtData> udtGen = integers().between(0, 500)
            .zip(strings().allPossible().ofLengthBetween(1, 15),
                 SimpleUdtData::new);

        Gen<TupleWithListUdtData> rowGen = integers().between(0, 1000)
            .zip(lists().of(udtGen).ofSizeBetween(1, 3),
                 TupleWithListUdtData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<TupleWithSetUdtData>> tupleWithSetUdtBatchGen()
    {
        Gen<SimpleUdtData> udtGen = integers().between(0, 500)
            .zip(strings().allPossible().ofLengthBetween(1, 15),
                 SimpleUdtData::new);

        Gen<TupleWithSetUdtData> rowGen = integers().between(0, 1000)
            .zip(lists().of(udtGen).ofSizeBetween(1, 3),
                 (i, list) -> new TupleWithSetUdtData(i, new HashSet<>(list)));

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<TupleWithMapUdtData>> tupleWithMapUdtBatchGen()
    {
        Gen<SimpleUdtData> udtGen = integers().between(0, 500)
            .zip(strings().allPossible().ofLengthBetween(1, 15),
                 SimpleUdtData::new);

        Gen<TupleWithMapUdtData> rowGen = integers().between(0, 1000)
            .zip(maps().of(strings().allPossible().ofLengthBetween(1, 10), udtGen).ofSizeBetween(1, 3),
                 TupleWithMapUdtData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<MultiTupleData>> multiTupleBatchGen()
    {
        Gen<TupleData> tuple1Gen = integers().between(0, 500)
            .zip(strings().allPossible().ofLengthBetween(1, 10),
                 TupleData::new);

        Gen<Tuple3Data> tuple2Gen = strings().allPossible().ofLengthBetween(1, 10)
            .zip(integers().between(0, 500),
                 integers().between(0, 1000).map(Integer::longValue),
                 Tuple3Data::new);

        Gen<TupleWithListAndSetData> tuple3Gen = lists().of(strings().allPossible().ofLengthBetween(1, 10))
            .ofSizeBetween(1, 3)
            .zip(lists().of(integers().between(0, 100)).ofSizeBetween(1, 3),
                 (list, setList) -> new TupleWithListAndSetData(list, new HashSet<>(setList)));

        Gen<MultiTupleData> rowGen = tuple1Gen.zip(tuple2Gen, tuple3Gen, MultiTupleData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<DeeplyNestedTupleData>> deeplyNestedTupleBatchGen()
    {
        Gen<Level3> level3Gen = integers().between(0, 1000).map(Integer::longValue)
                                          .zip(strings().allPossible().ofLengthBetween(1, 15),
                 Level3::new);

        Gen<Level2> level2Gen = strings().allPossible().ofLengthBetween(1, 15)
                                         .zip(level3Gen, Level2::new);

        Gen<DeeplyNestedTupleData> rowGen = integers().between(0, 1000)
            .zip(level2Gen, DeeplyNestedTupleData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<TupleAllCollectionsData>> tupleAllCollectionsBatchGen()
    {
        Gen<TupleAllCollectionsData> rowGen = lists().of(strings().allPossible().ofLengthBetween(1, 10))
            .ofSizeBetween(1, 3)
            .zip(lists().of(integers().between(0, 100)).ofSizeBetween(1, 3),
                 maps().of(strings().allPossible().ofLengthBetween(1, 10), integers().between(0, 100)).ofSizeBetween(1, 3),
                 (list, setList, map) -> new TupleAllCollectionsData(list, new HashSet<>(setList), map));

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<MapTupleKeyValueData>> mapTupleKeyValueBatchGen()
    {
        Gen<TupleData> keyGen = integers().between(0, 500)
            .zip(strings().allPossible().ofLengthBetween(1, 10),
                 TupleData::new);

        Gen<TupleData2> valueGen = strings().allPossible().ofLengthBetween(1, 10)
            .zip(integers().between(0, 500),
                 TupleData2::new);

        Gen<MapTupleKeyValueData> rowGen = maps().of(keyGen, valueGen)
            .ofSizeBetween(1, 3)
            .map(MapTupleKeyValueData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<TupleSetOfTuplesData>> tupleSetOfTuplesBatchGen()
    {
        Gen<TupleData2> innerTupleGen = strings().allPossible().ofLengthBetween(1, 10)
            .zip(integers().between(0, 500),
                 TupleData2::new);

        Gen<TupleSetOfTuplesData> rowGen = integers().between(0, 1000)
            .zip(lists().of(innerTupleGen).ofSizeBetween(1, 3),
                 (i, list) -> new TupleSetOfTuplesData(i, new HashSet<>(list)));

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<TupleNestedCollectionsData>> tupleNestedCollectionsBatchGen()
    {
        Gen<TupleNestedCollectionsData.InnerTuple> innerTupleGen = strings().allPossible().ofLengthBetween(1, 10)
            .zip(lists().of(integers().between(0, 100)).ofSizeBetween(1, 3),
                 (s, list) -> new TupleNestedCollectionsData.InnerTuple(s, new HashSet<>(list)));

        Gen<TupleNestedCollectionsData> rowGen = integers().between(0, 1000)
            .zip(lists().of(integers().between(0, 100)).ofSizeBetween(1, 3),
                 innerTupleGen,
                 maps().of(strings().allPossible().ofLengthBetween(1, 10), strings().allPossible().ofLengthBetween(1, 10)).ofSizeBetween(1, 3),
                 TupleNestedCollectionsData::new);

        return batchGenWithNulls(rowGen);
    }

    private Gen<List<TupleListOfTuplesData>> tupleListOfTuplesBatchGen()
    {
        Gen<TupleListOfTuplesData.InnerTuple> innerTupleGen = strings().allPossible().ofLengthBetween(1, 10)
            .zip(lists().of(integers().between(0, 100)).ofSizeBetween(1, 3),
                 (s, list) -> new TupleListOfTuplesData.InnerTuple(s, new HashSet<>(list)));

        Gen<TupleListOfTuplesData> rowGen = integers().between(0, 1000)
            .zip(lists().of(innerTupleGen).ofSizeBetween(1, 3),
                 TupleListOfTuplesData::new);

        return batchGenWithNulls(rowGen);
    }


    private Dataset<Row> createDataFrame(SparkSession spark, List<TupleData> tupleDataList)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < tupleDataList.size(); i++)
        {
            TupleData td = tupleDataList.get(i);
            rows.add(RowFactory.create(
                (long) i,
                td == null ? null : RowFactory.create(td.intVal, td.strVal)
            ));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createListOfTuplesDataFrame(SparkSession spark, List<ListOfTuplesData> dataList)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", DataTypes.createArrayType(tupleType), true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            ListOfTuplesData data = dataList.get(i);
            List<Row> tupleRows = data.tuples.stream()
                .map(t -> RowFactory.create(t.intVal, t.strVal))
                .collect(Collectors.toList());
            rows.add(RowFactory.create((long) i, tupleRows));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createMapWithTuplesDataFrame(SparkSession spark, List<MapWithTuplesData> dataList)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", DataTypes.createMapType(DataTypes.StringType, tupleType), true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            MapWithTuplesData data = dataList.get(i);
            Map<String, Row> tupleMap = new HashMap<>();
            for (Map.Entry<String, TupleData> entry : data.tuples.entrySet())
            {
                tupleMap.put(entry.getKey(), RowFactory.create(entry.getValue().intVal, entry.getValue().strVal));
            }
            rows.add(RowFactory.create((long) i, tupleMap));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createNestedTuplesDataFrame(SparkSession spark, List<NestedTupleData> dataList)
    {
        StructType innerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.StringType, false),
            DataTypes.createStructField("_2", DataTypes.IntegerType, false)
        ));

        StructType outerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", innerTupleType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", outerTupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            NestedTupleData data = dataList.get(i);
            Row innerTuple = RowFactory.create(data.inner.strVal, data.inner.intVal);
            Row outerTuple = RowFactory.create(data.outerInt, innerTuple);
            rows.add(RowFactory.create((long) i, outerTuple));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createUdtDataFrame(SparkSession spark, List<UdtData> dataList)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType udtType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("age", DataTypes.IntegerType, false),
            DataTypes.createStructField("address", tupleType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", udtType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            UdtData data = dataList.get(i);
            Row tupleRow = RowFactory.create(data.address.intVal, data.address.strVal);
            Row udtRow = RowFactory.create(data.name, data.age, tupleRow);
            rows.add(RowFactory.create((long) i, udtRow));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createSetOfTuplesDataFrame(SparkSession spark, List<SetOfTuplesData> dataList)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", DataTypes.createArrayType(tupleType), true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            SetOfTuplesData data = dataList.get(i);
            List<Row> tupleRows = data.tuples.stream()
                .map(t -> RowFactory.create(t.intVal, t.strVal))
                .collect(Collectors.toList());
            rows.add(RowFactory.create((long) i, tupleRows));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createMapWithTupleKeyDataFrame(SparkSession spark, List<MapWithTupleKeyData> dataList)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", DataTypes.createMapType(tupleType, DataTypes.StringType), true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            MapWithTupleKeyData data = dataList.get(i);
            Map<Row, String> tupleMap = new HashMap<>();
            for (Map.Entry<TupleData, String> entry : data.tuples.entrySet())
            {
                tupleMap.put(RowFactory.create(entry.getKey().intVal, entry.getKey().strVal), entry.getValue());
            }
            rows.add(RowFactory.create((long) i, tupleMap));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createTupleWithListDataFrame(SparkSession spark, List<TupleWithListData> dataList)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(DataTypes.StringType), false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            TupleWithListData data = dataList.get(i);
            Row tupleRow = RowFactory.create(data.intVal, data.list);
            rows.add(RowFactory.create((long) i, tupleRow));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createTupleWithSetDataFrame(SparkSession spark, List<TupleWithSetData> dataList)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(DataTypes.IntegerType), false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            TupleWithSetData data = dataList.get(i);
            Row tupleRow = RowFactory.create(data.intVal, new ArrayList<>(data.set));
            rows.add(RowFactory.create((long) i, tupleRow));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createTupleWithMapDataFrame(SparkSession spark, List<TupleWithMapData> dataList)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            TupleWithMapData data = dataList.get(i);
            Row tupleRow = RowFactory.create(data.intVal, data.map);
            rows.add(RowFactory.create((long) i, tupleRow));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createTupleWithUdtDataFrame(SparkSession spark, List<TupleWithUdtData> dataList)
    {
        StructType innerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType udtType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("f1", DataTypes.createArrayType(DataTypes.StringType), false),
            DataTypes.createStructField("f2", DataTypes.createArrayType(DataTypes.StringType), false),
            DataTypes.createStructField("f3", DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType), false),
            DataTypes.createStructField("f4", innerTupleType, false)
        ));

        StructType outerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", udtType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", outerTupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            TupleWithUdtData data = dataList.get(i);
            Row innerTuple = RowFactory.create(data.udt.tuple.intVal, data.udt.tuple.strVal);
            Row udt = RowFactory.create(data.udt.list, new ArrayList<>(data.udt.set), data.udt.map, innerTuple);
            Row outerTuple = RowFactory.create(data.intVal, udt);
            rows.add(RowFactory.create((long) i, outerTuple));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createTupleWithListUdtDataFrame(SparkSession spark, List<TupleWithListUdtData> dataList)
    {
        StructType udtType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("field1", DataTypes.IntegerType, false),
            DataTypes.createStructField("field2", DataTypes.StringType, false)
        ));

        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(udtType), false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            TupleWithListUdtData data = dataList.get(i);
            List<Row> udtList = data.udts.stream()
                .map(udt -> RowFactory.create(udt.field1, udt.field2))
                .collect(Collectors.toList());
            Row tupleRow = RowFactory.create(data.intVal, udtList);
            rows.add(RowFactory.create((long) i, tupleRow));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createTupleWithSetUdtDataFrame(SparkSession spark, List<TupleWithSetUdtData> dataList)
    {
        StructType udtType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("field1", DataTypes.IntegerType, false),
            DataTypes.createStructField("field2", DataTypes.StringType, false)
        ));

        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(udtType), false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            TupleWithSetUdtData data = dataList.get(i);
            List<Row> udtList = data.udts.stream()
                .map(udt -> RowFactory.create(udt.field1, udt.field2))
                .collect(Collectors.toList());
            Row tupleRow = RowFactory.create(data.intVal, udtList);
            rows.add(RowFactory.create((long) i, tupleRow));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createTupleWithMapUdtDataFrame(SparkSession spark, List<TupleWithMapUdtData> dataList)
    {
        StructType udtType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("field1", DataTypes.IntegerType, false),
            DataTypes.createStructField("field2", DataTypes.StringType, false)
        ));

        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createMapType(DataTypes.StringType, udtType), false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            TupleWithMapUdtData data = dataList.get(i);
            Map<String, Row> udtMap = new HashMap<>();
            for (Map.Entry<String, SimpleUdtData> entry : data.udts.entrySet())
            {
                udtMap.put(entry.getKey(), RowFactory.create(entry.getValue().field1, entry.getValue().field2));
            }
            Row tupleRow = RowFactory.create(data.intVal, udtMap);
            rows.add(RowFactory.create((long) i, tupleRow));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createMultiTupleDataFrame(SparkSession spark, List<MultiTupleData> dataList)
    {
        StructType tuple1Type = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType tuple2Type = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.StringType, false),
            DataTypes.createStructField("_2", DataTypes.IntegerType, false),
            DataTypes.createStructField("_3", DataTypes.LongType, false)
        ));

        StructType tuple3Type = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.createArrayType(DataTypes.StringType), false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(DataTypes.IntegerType), false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("tuple1", tuple1Type, false),
            DataTypes.createStructField("tuple2", tuple2Type, false),
            DataTypes.createStructField("tuple3", tuple3Type, false)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            MultiTupleData data = dataList.get(i);
            Row tuple1 = RowFactory.create(data.tuple1.intVal, data.tuple1.strVal);
            Row tuple2 = RowFactory.create(data.tuple2.str, data.tuple2.intVal, data.tuple2.longVal);
            Row tuple3 = RowFactory.create(data.tuple3.list, new ArrayList<>(data.tuple3.set));
            rows.add(RowFactory.create((long) i, tuple1, tuple2, tuple3));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createDeeplyNestedTupleDataFrame(SparkSession spark, List<DeeplyNestedTupleData> dataList)
    {
        StructType level3Type = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.LongType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType level2Type = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.StringType, false),
            DataTypes.createStructField("_2", level3Type, false)
        ));

        StructType level1Type = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", level2Type, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", level1Type, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            DeeplyNestedTupleData data = dataList.get(i);
            Row level3 = RowFactory.create(data.level2.level3.longVal, data.level2.level3.str);
            Row level2 = RowFactory.create(data.level2.str, level3);
            Row level1 = RowFactory.create(data.intVal, level2);
            rows.add(RowFactory.create((long) i, level1));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createTupleAllCollectionsDataFrame(SparkSession spark, List<TupleAllCollectionsData> dataList)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.createArrayType(DataTypes.StringType), false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(DataTypes.IntegerType), false),
            DataTypes.createStructField("_3", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            TupleAllCollectionsData data = dataList.get(i);
            Row tuple = RowFactory.create(data.list, new ArrayList<>(data.set), data.map);
            rows.add(RowFactory.create((long) i, tuple));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createMapTupleKeyValueDataFrame(SparkSession spark, List<MapTupleKeyValueData> dataList)
    {
        StructType keyTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType valueTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.StringType, false),
            DataTypes.createStructField("_2", DataTypes.IntegerType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", DataTypes.createMapType(keyTupleType, valueTupleType), true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            MapTupleKeyValueData data = dataList.get(i);
            Map<Row, Row> tupleMap = new HashMap<>();
            for (Map.Entry<TupleData, TupleData2> entry : data.map.entrySet())
            {
                Row keyTuple = RowFactory.create(entry.getKey().intVal, entry.getKey().strVal);
                Row valueTuple = RowFactory.create(entry.getValue().str, entry.getValue().intVal);
                tupleMap.put(keyTuple, valueTuple);
            }
            rows.add(RowFactory.create((long) i, tupleMap));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createTupleSetOfTuplesDataFrame(SparkSession spark, List<TupleSetOfTuplesData> dataList)
    {
        StructType innerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.StringType, false),
            DataTypes.createStructField("_2", DataTypes.IntegerType, false)
        ));

        StructType outerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(innerTupleType), false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", outerTupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            TupleSetOfTuplesData data = dataList.get(i);
            List<Row> innerTuples = data.tuples.stream()
                .map(t -> RowFactory.create(t.str, t.intVal))
                .collect(Collectors.toList());
            Row outerTuple = RowFactory.create(data.intVal, innerTuples);
            rows.add(RowFactory.create((long) i, outerTuple));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createTupleNestedCollectionsDataFrame(SparkSession spark, List<TupleNestedCollectionsData> dataList)
    {
        StructType innerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.StringType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(DataTypes.IntegerType), false)
        ));

        StructType outerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(DataTypes.IntegerType), false),
            DataTypes.createStructField("_3", innerTupleType, false),
            DataTypes.createStructField("_4", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", outerTupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            TupleNestedCollectionsData data = dataList.get(i);
            Row innerTuple = RowFactory.create(data.innerTuple.str, new ArrayList<>(data.innerTuple.set));
            Row outerTuple = RowFactory.create(data.intVal, data.list, innerTuple, data.map);
            rows.add(RowFactory.create((long) i, outerTuple));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createTupleListOfTuplesDataFrame(SparkSession spark, List<TupleListOfTuplesData> dataList)
    {
        StructType innerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.StringType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(DataTypes.IntegerType), false)
        ));

        StructType outerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(innerTupleType), false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", outerTupleType, true)
        ));

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < dataList.size(); i++)
        {
            TupleListOfTuplesData data = dataList.get(i);
            List<Row> innerTuples = data.tuples.stream()
                .map(t -> RowFactory.create(t.str, new ArrayList<>(t.set)))
                .collect(Collectors.toList());
            Row outerTuple = RowFactory.create(data.intVal, innerTuples);
            rows.add(RowFactory.create((long) i, outerTuple));
        }

        return spark.createDataFrame(rows, schema);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration().nodesPerDc(3);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);

        CassandraBridge bridge = getOrCreateBridge();

        // Create UDTs
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TYPE %s.udt_with_collections (f1 list<text>, f2 set<text>, f3 map<int, text>, f4 tuple<int, text>)",
            TEST_KEYSPACE
        ));

        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TYPE %s.simple_udt (field1 int, field2 text)",
            TEST_KEYSPACE
        ));

        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TYPE %s.person (name text, age int, address frozen<tuple<int, text>>)",
            TEST_KEYSPACE
        ));

        // Simple tuple
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, text>>)",
            TUPLE_TABLE.keyspace(),
            TUPLE_TABLE.table()
        ));

        // List of tuples
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data list<frozen<tuple<int, text>>>)",
            LIST_OF_TUPLES_TABLE.keyspace(),
            LIST_OF_TUPLES_TABLE.table()
        ));

        // Set of tuples
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data set<frozen<tuple<int, text>>>)",
            SET_OF_TUPLES_TABLE.keyspace(),
            SET_OF_TUPLES_TABLE.table()
        ));

        // Map with tuple values
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data map<text, frozen<tuple<int, text>>>)",
            MAP_WITH_TUPLES_TABLE.keyspace(),
            MAP_WITH_TUPLES_TABLE.table()
        ));

        // Map with tuple keys
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data map<frozen<tuple<int, text>>, text>)",
            MAP_WITH_TUPLE_KEY_TABLE.keyspace(),
            MAP_WITH_TUPLE_KEY_TABLE.table()
        ));

        // Nested tuple
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, frozen<tuple<text, int>>>>)",
            NESTED_TUPLE_TABLE.keyspace(),
            NESTED_TUPLE_TABLE.table()
        ));

        // UDT with tuple
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<person>)",
            UDT_TABLE.keyspace(),
            UDT_TABLE.table()
        ));

        // Tuple with list
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, list<text>>>)",
            TUPLE_WITH_LIST_TABLE.keyspace(),
            TUPLE_WITH_LIST_TABLE.table()
        ));

        // Tuple with set
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, set<int>>>)",
            TUPLE_WITH_SET_TABLE.keyspace(),
            TUPLE_WITH_SET_TABLE.table()
        ));

        // Tuple with map
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, map<text, int>>>)",
            TUPLE_WITH_MAP_TABLE.keyspace(),
            TUPLE_WITH_MAP_TABLE.table()
        ));

        // Tuple with UDT with collections
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, frozen<udt_with_collections>>>)",
            TUPLE_WITH_UDT_TABLE.keyspace(),
            TUPLE_WITH_UDT_TABLE.table()
        ));

        // Tuple with list of UDT
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, list<frozen<simple_udt>>>>)",
            TUPLE_WITH_LIST_UDT_TABLE.keyspace(),
            TUPLE_WITH_LIST_UDT_TABLE.table()
        ));

        // Tuple with set of UDT
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, set<frozen<simple_udt>>>>)",
            TUPLE_WITH_SET_UDT_TABLE.keyspace(),
            TUPLE_WITH_SET_UDT_TABLE.table()
        ));

        // Tuple with map of UDT
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, map<text, frozen<simple_udt>>>>)",
            TUPLE_WITH_MAP_UDT_TABLE.keyspace(),
            TUPLE_WITH_MAP_UDT_TABLE.table()
        ));

        // Multiple tuple columns
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, " +
            "tuple1 frozen<tuple<int, text>>, " +
            "tuple2 frozen<tuple<text, int, bigint>>, " +
            "tuple3 frozen<tuple<list<text>, set<int>>>)",
            MULTI_TUPLE_TABLE.keyspace(),
            MULTI_TUPLE_TABLE.table()
        ));

        // Deeply nested tuple (3 levels)
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, " +
            "data frozen<tuple<int, frozen<tuple<text, frozen<tuple<bigint, text>>>>>>)",
            DEEPLY_NESTED_TUPLE_TABLE.keyspace(),
            DEEPLY_NESTED_TUPLE_TABLE.table()
        ));

        // Tuple with all collection types
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, " +
            "data frozen<tuple<list<text>, set<int>, map<text, int>>>)",
            TUPLE_ALL_COLLECTIONS_TABLE.keyspace(),
            TUPLE_ALL_COLLECTIONS_TABLE.table()
        ));

        // Map with tuple key and value
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, " +
            "data map<frozen<tuple<int, text>>, frozen<tuple<text, int>>>)",
            MAP_TUPLE_KEY_VALUE_TABLE.keyspace(),
            MAP_TUPLE_KEY_VALUE_TABLE.table()
        ));

        // Tuple with set of tuples
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, " +
            "data frozen<tuple<int, set<frozen<tuple<text, int>>>>>)",
            TUPLE_SET_OF_TUPLES_TABLE.keyspace(),
            TUPLE_SET_OF_TUPLES_TABLE.table()
        ));

        // Tuple with nested collections
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, " +
            "data frozen<tuple<int, list<int>, frozen<tuple<text, set<int>>>, map<text, text>>>)",
            TUPLE_NESTED_COLLECTIONS_TABLE.keyspace(),
            TUPLE_NESTED_COLLECTIONS_TABLE.table()
        ));

        // Tuple with list of tuples
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, " +
            "data frozen<tuple<int, list<frozen<tuple<text, set<int>>>>>>)",
            TUPLE_LIST_OF_TUPLES_TABLE.keyspace(),
            TUPLE_LIST_OF_TUPLES_TABLE.table()
        ));

        // Create tables for all supported types - parameterized tests
        // These are created dynamically for each supported type from the bridge
        for (CqlField.CqlType type : bridge.supportedTypes())
        {
            String sanitizedName = sanitizeTypeName(type.cqlName());

            try
            {
                // tuple<int, type, bigint>
                cluster.schemaChangeIgnoringStoppedInstances(String.format(
                    "CREATE TABLE %s.qt_all_types_%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, %s, bigint>>)",
                    TEST_KEYSPACE, sanitizedName, type.cqlName()
                ));
            }
            catch (Exception e)
            {
                // Skip if table already exists or type not supported in this context
            }

            try
            {
                // list<frozen<tuple<int, type>>>
                cluster.schemaChangeIgnoringStoppedInstances(String.format(
                    "CREATE TABLE %s.qt_list_tuple_%s (id BIGINT PRIMARY KEY, data list<frozen<tuple<int, %s>>>)",
                    TEST_KEYSPACE, sanitizedName, type.cqlName()
                ));
            }
            catch (Exception e)
            {
                // Skip if not supported
            }

            if (type.supportedAsSetElement())
            {
                try
                {
                    // set<frozen<tuple<type, int>>>
                    cluster.schemaChangeIgnoringStoppedInstances(String.format(
                        "CREATE TABLE %s.qt_set_tuple_%s (id BIGINT PRIMARY KEY, data set<frozen<tuple<%s, int>>>)",
                        TEST_KEYSPACE, sanitizedName, type.cqlName()
                    ));
                }
                catch (Exception e)
                {
                    // Skip if not supported
                }

                try
                {
                    // tuple<int, set<type>>
                    cluster.schemaChangeIgnoringStoppedInstances(String.format(
                        "CREATE TABLE %s.qt_tuple_set_all_%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, set<%s>>>)",
                        TEST_KEYSPACE, sanitizedName, type.cqlName()
                    ));
                }
                catch (Exception e)
                {
                    // Skip if not supported
                }
            }

            try
            {
                // tuple<int, list<type>>
                cluster.schemaChangeIgnoringStoppedInstances(String.format(
                    "CREATE TABLE %s.qt_tuple_list_all_%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, list<%s>>>)",
                    TEST_KEYSPACE, sanitizedName, type.cqlName()
                ));
            }
            catch (Exception e)
            {
                // Skip if not supported
            }

            try
            {
                // tuple<int, frozen<tuple<type, bigint>>>
                cluster.schemaChangeIgnoringStoppedInstances(String.format(
                    "CREATE TABLE %s.qt_nested_all_%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, frozen<tuple<%s, bigint>>>>)",
                    TEST_KEYSPACE, sanitizedName, type.cqlName()
                ));
            }
            catch (Exception e)
            {
                // Skip if not supported
            }
        }
    }

    /**
     * Simple holder for tuple data generated by QuickTheories.
     */
    static class TupleData
    {
        final int intVal;
        final String strVal;

        TupleData(int intVal, String strVal)
        {
            this.intVal = intVal;
            this.strVal = strVal;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) return true;
            if (!(obj instanceof TupleData)) return false;
            TupleData that = (TupleData) obj;
            return intVal == that.intVal && strVal.equals(that.strVal);
        }

        @Override
        public int hashCode()
        {
            return intVal * 31 + strVal.hashCode();
        }

        @Override
        public String toString()
        {
            return String.format("TupleData(intVal=%d, strVal='%s')", intVal, strVal);
        }
    }

    static class ListOfTuplesData
    {
        final List<TupleData> tuples;

        ListOfTuplesData(List<TupleData> tuples)
        {
            this.tuples = tuples;
        }

        @Override
        public String toString()
        {
            return String.format("ListOfTuplesData(tuples=%s)", tuples);
        }
    }

    static class MapWithTuplesData
    {
        final Map<String, TupleData> tuples;

        MapWithTuplesData(Map<String, TupleData> tuples)
        {
            this.tuples = tuples;
        }
    }

    static class NestedTupleData
    {
        final int outerInt;
        final InnerTuple inner;

        NestedTupleData(int outerInt, InnerTuple inner)
        {
            this.outerInt = outerInt;
            this.inner = inner;
        }

        @Override
        public String toString()
        {
            return String.format("NestedTupleData(outerInt=%d, inner=%s)", outerInt, inner);
        }

        static class InnerTuple
        {
            final String strVal;
            final int intVal;

            InnerTuple(String strVal, int intVal)
            {
                this.strVal = strVal;
                this.intVal = intVal;
            }

            @Override
            public String toString()
            {
                return String.format("InnerTuple(strVal='%s', intVal=%d)", strVal, intVal);
            }
        }
    }

    static class UdtData
    {
        final String name;
        final int age;
        final TupleData address;

        UdtData(String name, int age, TupleData address)
        {
            this.name = name;
            this.age = age;
            this.address = address;
        }
    }

    static class SetOfTuplesData
    {
        final Set<TupleData> tuples;

        SetOfTuplesData(Set<TupleData> tuples)
        {
            this.tuples = tuples;
        }
    }

    static class MapWithTupleKeyData
    {
        final Map<TupleData, String> tuples;

        MapWithTupleKeyData(Map<TupleData, String> tuples)
        {
            this.tuples = tuples;
        }
    }

    static class TupleWithListData
    {
        final int intVal;
        final List<String> list;

        TupleWithListData(int intVal, List<String> list)
        {
            this.intVal = intVal;
            this.list = list;
        }
    }

    static class TupleWithSetData
    {
        final int intVal;
        final Set<Integer> set;

        TupleWithSetData(int intVal, Set<Integer> set)
        {
            this.intVal = intVal;
            this.set = set;
        }
    }

    static class TupleWithMapData
    {
        final int intVal;
        final Map<String, Integer> map;

        TupleWithMapData(int intVal, Map<String, Integer> map)
        {
            this.intVal = intVal;
            this.map = map;
        }
    }

    static class UdtWithCollectionsData
    {
        final List<String> list;
        final Set<String> set;
        final Map<Integer, String> map;
        final TupleData tuple;

        UdtWithCollectionsData(List<String> list, List<String> setList, Map<Integer, String> map, TupleData tuple)
        {
            this.list = list;
            this.set = new HashSet<>(setList);
            this.map = map;
            this.tuple = tuple;
        }
    }

    static class TupleWithUdtData
    {
        final int intVal;
        final UdtWithCollectionsData udt;

        TupleWithUdtData(int intVal, UdtWithCollectionsData udt)
        {
            this.intVal = intVal;
            this.udt = udt;
        }
    }

    static class SimpleUdtData
    {
        final int field1;
        final String field2;

        SimpleUdtData(int field1, String field2)
        {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) return true;
            if (!(obj instanceof SimpleUdtData)) return false;
            SimpleUdtData that = (SimpleUdtData) obj;
            return field1 == that.field1 && field2.equals(that.field2);
        }

        @Override
        public int hashCode()
        {
            return field1 * 31 + field2.hashCode();
        }
    }

    static class TupleWithListUdtData
    {
        final int intVal;
        final List<SimpleUdtData> udts;

        TupleWithListUdtData(int intVal, List<SimpleUdtData> udts)
        {
            this.intVal = intVal;
            this.udts = udts;
        }
    }

    static class TupleWithSetUdtData
    {
        final int intVal;
        final Set<SimpleUdtData> udts;

        TupleWithSetUdtData(int intVal, Set<SimpleUdtData> udts)
        {
            this.intVal = intVal;
            this.udts = udts;
        }
    }

    static class TupleWithMapUdtData
    {
        final int intVal;
        final Map<String, SimpleUdtData> udts;

        TupleWithMapUdtData(int intVal, Map<String, SimpleUdtData> udts)
        {
            this.intVal = intVal;
            this.udts = udts;
        }
    }

    static class Tuple3Data
    {
        final String str;
        final int intVal;
        final long longVal;

        Tuple3Data(String str, int intVal, long longVal)
        {
            this.str = str;
            this.intVal = intVal;
            this.longVal = longVal;
        }
    }

    static class TupleWithListAndSetData
    {
        final List<String> list;
        final Set<Integer> set;

        TupleWithListAndSetData(List<String> list, Set<Integer> set)
        {
            this.list = list;
            this.set = set;
        }
    }

    static class MultiTupleData
    {
        final TupleData tuple1;
        final Tuple3Data tuple2;
        final TupleWithListAndSetData tuple3;

        MultiTupleData(TupleData tuple1, Tuple3Data tuple2, TupleWithListAndSetData tuple3)
        {
            this.tuple1 = tuple1;
            this.tuple2 = tuple2;
            this.tuple3 = tuple3;
        }
    }

    static class DeeplyNestedTupleData
    {
        final int intVal;
        final Level2 level2;

        DeeplyNestedTupleData(int intVal, Level2 level2)
        {
            this.intVal = intVal;
            this.level2 = level2;
        }

        @Override
        public String toString()
        {
            return String.format("DeeplyNestedTupleData(intVal=%d, level2=%s)", intVal, level2);
        }

        static class Level2
        {
            final String str;
            final Level3 level3;

            Level2(String str, Level3 level3)
            {
                this.str = str;
                this.level3 = level3;
            }

            @Override
            public String toString()
            {
                return String.format("Level2(str='%s', level3=%s)", str, level3);
            }
        }

        static class Level3
        {
            final long longVal;
            final String str;

            Level3(long longVal, String str)
            {
                this.longVal = longVal;
                this.str = str;
            }

            @Override
            public String toString()
            {
                return String.format("Level3(longVal=%d, str='%s')", longVal, str);
            }
        }
    }

    static class TupleAllCollectionsData
    {
        final List<String> list;
        final Set<Integer> set;
        final Map<String, Integer> map;

        TupleAllCollectionsData(List<String> list, Set<Integer> set, Map<String, Integer> map)
        {
            this.list = list;
            this.set = set;
            this.map = map;
        }
    }

    static class TupleData2
    {
        final String str;
        final int intVal;

        TupleData2(String str, int intVal)
        {
            this.str = str;
            this.intVal = intVal;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) return true;
            if (!(obj instanceof TupleData2)) return false;
            TupleData2 that = (TupleData2) obj;
            return intVal == that.intVal && str.equals(that.str);
        }

        @Override
        public int hashCode()
        {
            return intVal * 31 + str.hashCode();
        }
    }

    static class MapTupleKeyValueData
    {
        final Map<TupleData, TupleData2> map;

        MapTupleKeyValueData(Map<TupleData, TupleData2> map)
        {
            this.map = map;
        }
    }

    static class TupleSetOfTuplesData
    {
        final int intVal;
        final Set<TupleData2> tuples;

        TupleSetOfTuplesData(int intVal, Set<TupleData2> tuples)
        {
            this.intVal = intVal;
            this.tuples = tuples;
        }
    }

    static class TupleNestedCollectionsData
    {
        final int intVal;
        final List<Integer> list;
        final InnerTuple innerTuple;
        final Map<String, String> map;

        TupleNestedCollectionsData(int intVal, List<Integer> list, InnerTuple innerTuple, Map<String, String> map)
        {
            this.intVal = intVal;
            this.list = list;
            this.innerTuple = innerTuple;
            this.map = map;
        }

        static class InnerTuple
        {
            final String str;
            final Set<Integer> set;

            InnerTuple(String str, Set<Integer> set)
            {
                this.str = str;
                this.set = set;
            }
        }
    }

    static class TupleListOfTuplesData
    {
        final int intVal;
        final List<InnerTuple> tuples;

        TupleListOfTuplesData(int intVal, List<InnerTuple> tuples)
        {
            this.intVal = intVal;
            this.tuples = tuples;
        }

        static class InnerTuple
        {
            final String str;
            final Set<Integer> set;

            InnerTuple(String str, Set<Integer> set)
            {
                this.str = str;
                this.set = set;
            }
        }
    }

    // ==================== Assertion Context Formatting Methods ====================

    /**
     * Formats a simple tuple&lt;int, text&gt; for display.
     */
    private String formatSimpleTuple(Row tuple)
    {
        if (tuple == null) return "NULL";
        return String.format("<%d, '%s'>", tuple.getInt(0), tuple.getString(1));
    }

    /**
     * Formats a tuple test row for display in assertion messages.
     * Row(id, frozen&lt;tuple&lt;int, text&gt;&gt;)
     */
    private String formatTupleRow(Row row)
    {
        if (row == null) return "NULL";
        long id = row.getLong(0);
        if (row.isNullAt(1)) return String.format("Row(id=%d, data=NULL)", id);
        return String.format("Row(id=%d, data=%s)", id, formatSimpleTuple(row.getStruct(1)));
    }

    /**
     * Generic formatter that includes row index and both rows.
     */
    private String formatContext(int rowIndex, String sourceFormatted, String readFormatted)
    {
        return String.format("Row %d mismatch\nSource: %s\nRead:   %s",
            rowIndex, sourceFormatted, readFormatted);
    }

    /**
     * Generic row formatter - formats any Row with id and data columns.
     * Falls back to toString() for complex data types.
     */
    private String formatGenericRow(Row row)
    {
        if (row == null) return "NULL";
        long id = row.getLong(0);
        if (row.isNullAt(1)) return String.format("Row(id=%d, data=NULL)", id);
        Object data = row.get(1);
        String dataStr = data.toString();
        if (dataStr.length() > 200) dataStr = dataStr.substring(0, 197) + "...";
        return String.format("Row(id=%d, data=%s)", id, dataStr);
    }
}
