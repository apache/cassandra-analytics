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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

/**
 * Property-based testing for tuple types.
 */
class BulkWriteTupleTest extends SharedClusterSparkIntegrationTestBase
{
    // Number of rows to test per test method
    private static final int NUM_ROWS = 50;

    // Probability of null values
    private static final double NULL_PROBABILITY = 0.2;

    // Minimum number of null rows guaranteed in each test batch
    private static final int MIN_NULL_ROWS = 2;

    private static final QualifiedName TUPLE_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuples");
    private static final QualifiedName LIST_OF_TUPLES_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_list_tuples");
    private static final QualifiedName SET_OF_TUPLES_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_set_tuples");
    private static final QualifiedName NESTED_TUPLE_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_nested_tuples");
    private static final QualifiedName TUPLE_WITH_LIST_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_list");
    private static final QualifiedName TUPLE_WITH_SET_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_set");
    private static final QualifiedName TUPLE_WITH_MAP_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_map");
    private static final QualifiedName TUPLE_WITH_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_udt");
    private static final QualifiedName MAP_TUPLE_KEY_VALUE_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_map_tuple_kv");
    private static final QualifiedName TUPLE_SET_OF_TUPLES_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_set_tuples");
    private static final QualifiedName TUPLE_LIST_OF_TUPLES_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_list_tuples");

    /**
     * Tests: Basic two-field tuple
     * <p>Table: CREATE TABLE qt_tuples (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, text&gt;&gt;)
     */
    @Test
    void testSimpleTwoFieldTuple()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateTupleDataFrame(spark, seed);
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
            });
    }

    /**
     * Tests: Nested tuples with 2 levels of nesting (tests nulls at both levels)
     * <p>Table: CREATE TABLE qt_nested_tuples (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, frozen&lt;tuple&lt;text, int&gt;&gt;&gt;&gt;)
     */
    @Test
    void testNestedTuplesWithTwoLevels()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateNestedTuplesDataFrame(spark, seed);
                truncateTable(NESTED_TUPLE_TABLE);

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

                        // Handle null inner tuple
                        if (sourceOuter.isNullAt(1))
                        {
                            assertThat(readOuter.isNullAt(1))
                            .as(context + " (inner tuple)")
                            .isTrue();
                        }
                        else
                        {
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
                }
                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: List of tuples (tests variable length, null lists, and null tuples within lists)
     * <p>Table: CREATE TABLE qt_list_tuples (id BIGINT PRIMARY KEY, data list&lt;frozen&lt;tuple&lt;int, text&gt;&gt;&gt;)
     */
    @Test
    void testListOfTuples()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateListOfTuplesDataFrame(spark, seed);
                truncateTable(LIST_OF_TUPLES_TABLE);

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
                        // Cassandra stores empty collections as NULL
                        if (sourceList.isEmpty())
                        {
                            assertThat(readRow.isNullAt(1)).as(context).isTrue();
                        }
                        else
                        {
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
                }
                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Set of tuples
     * <p>Table: CREATE TABLE qt_set_tuples (id BIGINT PRIMARY KEY, data set&lt;frozen&lt;tuple&lt;int, text&gt;&gt;&gt;)
     */
    @Test
    void testSetOfTuples()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateSetOfTuplesDataFrame(spark, seed);
                truncateTable(SET_OF_TUPLES_TABLE);

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
                        // Cassandra stores empty collections as NULL
                        if (sourceSet.isEmpty())
                        {
                            assertThat(readRow.isNullAt(1)).as(context).isTrue();
                        }
                        else
                        {
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
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Map with tuples as both keys and values, tuple comparison for keys
     * <p>Table: CREATE TABLE qt_map_tuple_kv (id BIGINT PRIMARY KEY,
     *          data map&lt;frozen&lt;tuple&lt;int, text&gt;&gt;, frozen&lt;tuple&lt;text, int&gt;&gt;&gt;)
     */
    @Test
    void testMapWithTupleKeysAndTupleValues()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateMapTupleKeyValueDataFrame(spark, seed);
                truncateTable(MAP_TUPLE_KEY_VALUE_TABLE);

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
                        // Cassandra stores empty collections as NULL
                        if (sourceMap.isEmpty())
                        {
                            assertThat(readRow.isNullAt(1)).as(context).isTrue();
                        }
                        else
                        {
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
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Tuple containing a list
     * <p>Table: CREATE TABLE qt_tuple_list (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, list&lt;text&gt;&gt;&gt;)
     */
    @Test
    void testTupleContainingList()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateTupleWithListDataFrame(spark, seed);
                truncateTable(TUPLE_WITH_LIST_TABLE);

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
                        // Handle null lists within tuple
                        if (sourceTuple.isNullAt(1))
                        {
                            assertThat(readTuple.isNullAt(1))
                                .as(context + " (list)")
                                .isTrue();
                        }
                        else
                        {
                            assertThat(readTuple.getList(1))
                                .as(context)
                                .isEqualTo(sourceTuple.getList(1));
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Tuple containing a set
     * <p>Table: CREATE TABLE qt_tuple_set (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, set&lt;int&gt;&gt;&gt;)
     */
    @Test
    void testTupleContainingSet()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateTupleWithSetDataFrame(spark, seed);
                truncateTable(TUPLE_WITH_SET_TABLE);

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
                        // Handle null sets within tuple
                        if (sourceTuple.isNullAt(1))
                        {
                            assertThat(readTuple.isNullAt(1))
                                .as(context + " (set)")
                                .isTrue();
                        }
                        else
                        {
                            // Sets may be in different order, so compare as sets
                            Set<Integer> sourceSet = new HashSet<>(sourceTuple.getList(1));
                            Set<Integer> readSet = new HashSet<>(readTuple.getList(1));
                            assertThat(readSet)
                                .as(context)
                                .isEqualTo(sourceSet);
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Tuple containing a map
     * <p>Table: CREATE TABLE qt_tuple_map (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, map&lt;text, int&gt;&gt;&gt;)
     */
    @Test
    void testTupleContainingMap()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateTupleWithMapDataFrame(spark, seed);
                truncateTable(TUPLE_WITH_MAP_TABLE);

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
                        // Handle null maps within tuple
                        if (sourceTuple.isNullAt(1))
                        {
                            assertThat(readTuple.isNullAt(1))
                                .as(context + " (map)")
                                .isTrue();
                        }
                        else
                        {
                            assertThat(readTuple.getJavaMap(1))
                                .as(context)
                                .isEqualTo(sourceTuple.getJavaMap(1));
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Tuple containing UDT with collections and nested tuple
     * <p>Table: CREATE TABLE qt_tuple_udt (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, frozen&lt;udt_with_collections&gt;&gt;&gt;)
     */
    @Test
    void testTupleContainingUdtWithCollections()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateTupleWithUdtDataFrame(spark, seed);
                truncateTable(TUPLE_WITH_UDT_TABLE);

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

                        // Handle null collection fields in UDT
                        if (sourceUdt.isNullAt(0))
                        {
                            assertThat(readUdt.isNullAt(0)).as(context + " (list)").isTrue();
                        }
                        else
                        {
                            assertThat(readUdt.getList(0)).as(context).isEqualTo(sourceUdt.getList(0));
                        }

                        if (sourceUdt.isNullAt(1))
                        {
                            assertThat(readUdt.isNullAt(1)).as(context + " (set)").isTrue();
                        }
                        else
                        {
                            assertThat(new HashSet<>(readUdt.getList(1))).as(context).isEqualTo(new HashSet<>(sourceUdt.getList(1)));
                        }

                        if (sourceUdt.isNullAt(2))
                        {
                            assertThat(readUdt.isNullAt(2)).as(context + " (map)").isTrue();
                        }
                        else
                        {
                            assertThat(readUdt.getJavaMap(2)).as(context).isEqualTo(sourceUdt.getJavaMap(2));
                        }

                        Row sourceTupleInUdt = sourceUdt.getStruct(3);
                        Row readTupleInUdt = readUdt.getStruct(3);
                        assertThat(readTupleInUdt.getInt(0)).as(context).isEqualTo(sourceTupleInUdt.getInt(0));
                        assertThat(readTupleInUdt.getString(1)).as(context).isEqualTo(sourceTupleInUdt.getString(1));
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Tuple containing a set of nested tuples with deduplication
     * <p>Table: CREATE TABLE qt_tuple_set_tuples (id BIGINT PRIMARY KEY, data frozen&lt;tuple&lt;int, set&lt;frozen&lt;tuple&lt;text, int&gt;&gt;&gt;&gt;&gt;)
     */
    @Test
    void testTupleContainingSetOfTuples()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateTupleSetOfTuplesDataFrame(spark, seed);
                truncateTable(TUPLE_SET_OF_TUPLES_TABLE);

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

                        // Handle null set inside tuple
                        if (sourceTuple.isNullAt(1))
                        {
                            assertThat(readTuple.isNullAt(1)).as(context + " (set)").isTrue();
                        }
                        else
                        {
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
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Tuple containing a list of nested tuples, each with a set field
     * <p>Table: CREATE TABLE qt_tuple_list_tuples (id BIGINT PRIMARY KEY,
     *          data frozen&lt;tuple&lt;int, list&lt;frozen&lt;tuple&lt;text, set&lt;int&gt;&gt;&gt;&gt;&gt;&gt;)
     */
    @Test
    void testTupleContainingListOfTuples()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateTupleListOfTuplesDataFrame(spark, seed);
                truncateTable(TUPLE_LIST_OF_TUPLES_TABLE);

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

                        // Handle null list inside tuple
                        if (sourceTuple.isNullAt(1))
                        {
                            assertThat(readTuple.isNullAt(1)).as(context + " (list)").isTrue();
                        }
                        else
                        {
                            List<Row> sourceList = sourceTuple.getList(1);
                            List<Row> readList = readTuple.getList(1);
                            assertThat(readList).as(context)
                            .hasSize(sourceList.size());

                            for (int j = 0; j < sourceList.size(); j++)
                            {
                                Row sourceNestedTuple = sourceList.get(j);
                                Row readNestedTuple = readList.get(j);
                                assertThat(readNestedTuple.getString(0)).as(context).isEqualTo(sourceNestedTuple.getString(0));
                                // Handle null set inside nested tuple
                                if (sourceNestedTuple.isNullAt(1))
                                {
                                    assertThat(readNestedTuple.isNullAt(1)).as(context + " (set in nested tuple " + j + ")").isTrue();
                                }
                                else
                                {
                                    assertThat(new HashSet<>(readNestedTuple.getList(1))).as(context).isEqualTo(new HashSet<>(sourceNestedTuple.getList(1)));
                                }
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    private void truncateTable(QualifiedName tableName)
    {
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "TRUNCATE %s.%s",
            TEST_KEYSPACE, tableName.table()));
    }

    /**
     * Generates a random string of lowercase letters.
     */
    private String randomString(java.util.Random rnd)
    {
        int length = rnd.nextInt(20) + 1; // 1-20 characters (never empty)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
        {
            sb.append((char) ('a' + rnd.nextInt(26)));
        }
        return sb.toString();
    }

    // ==================== Generate DataFrame Methods ====================

    /**
     * Generates a DataFrame for tuple&lt;int, text&gt;.
     */
    private Dataset<Row> generateTupleDataFrame(SparkSession spark, long seed)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        java.util.Random rnd = new java.util.Random(seed);
        List<Row> rows = new ArrayList<>();
        long rowId = 0;

        for (int i = 0; i < NUM_ROWS; i++)
        {
            if (rnd.nextInt(100) < NULL_PROBABILITY * 100)
            {
                rows.add(RowFactory.create(rowId++, null));
            }
            else
            {
                int intVal = rnd.nextInt(1001);
                String strVal = randomString(rnd);
                rows.add(RowFactory.create(rowId++, RowFactory.create(intVal, strVal)));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates a DataFrame for list&lt;frozen&lt;tuple&lt;int, text&gt;&gt;&gt;.
     */
    private Dataset<Row> generateListOfTuplesDataFrame(SparkSession spark, long seed)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", DataTypes.createArrayType(tupleType), true)
        ));

        java.util.Random rnd = new java.util.Random(seed);
        List<Row> rows = new ArrayList<>();
        long rowId = 0;

        for (int i = 0; i < NUM_ROWS; i++)
        {
            if (rnd.nextInt(100) < NULL_PROBABILITY * 100)
            {
                rows.add(RowFactory.create(rowId++, null));
            }
            else
            {
                int listSize = rnd.nextInt(6);
                List<Row> tupleRows = new ArrayList<>();
                for (int j = 0; j < listSize; j++)
                {
                    tupleRows.add(RowFactory.create(rnd.nextInt(501), randomString(rnd)));
                }
                rows.add(RowFactory.create(rowId++, tupleRows));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates a DataFrame for tuple&lt;int, tuple&lt;text, int&gt;&gt;.
     */
    private Dataset<Row> generateNestedTuplesDataFrame(SparkSession spark, long seed)
    {
        StructType innerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.StringType, false),
            DataTypes.createStructField("_2", DataTypes.IntegerType, false)
        ));

        StructType outerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", innerTupleType, true)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", outerTupleType, true)
        ));

        java.util.Random rnd = new java.util.Random(seed);
        List<Row> rows = new ArrayList<>();
        long rowId = 0;

        for (int i = 0; i < NUM_ROWS; i++)
        {
            if (rnd.nextInt(100) < NULL_PROBABILITY * 100)
            {
                rows.add(RowFactory.create(rowId++, null));
            }
            else
            {
                int outerInt = rnd.nextInt(1001);
                Row innerTuple = null;
                if (rnd.nextInt(100) >= 20)
                {
                    innerTuple = RowFactory.create(randomString(rnd), rnd.nextInt(501));
                }
                Row outerTuple = RowFactory.create(outerInt, innerTuple);
                rows.add(RowFactory.create(rowId++, outerTuple));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates a DataFrame for set&lt;frozen&lt;tuple&lt;int, text&gt;&gt;&gt;.
     */
    private Dataset<Row> generateSetOfTuplesDataFrame(SparkSession spark, long seed)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", DataTypes.createArrayType(tupleType), true)
        ));

        java.util.Random rnd = new java.util.Random(seed);
        List<Row> rows = new ArrayList<>();
        long rowId = 0;

        for (int i = 0; i < NUM_ROWS; i++)
        {
            if (rnd.nextInt(100) < NULL_PROBABILITY * 100)
            {
                rows.add(RowFactory.create(rowId++, null));
            }
            else
            {
                int setSize = rnd.nextInt(6);
                List<Row> tupleRows = new ArrayList<>();
                for (int j = 0; j < setSize; j++)
                {
                    tupleRows.add(RowFactory.create(rnd.nextInt(501), randomString(rnd)));
                }
                rows.add(RowFactory.create(rowId++, tupleRows));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates a DataFrame for tuple&lt;int, list&lt;text&gt;&gt;.
     */
    private Dataset<Row> generateTupleWithListDataFrame(SparkSession spark, long seed)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(DataTypes.StringType), true)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        java.util.Random rnd = new java.util.Random(seed);
        List<Row> rows = new ArrayList<>();
        long rowId = 0;

        for (int i = 0; i < NUM_ROWS; i++)
        {
            if (rnd.nextInt(100) < NULL_PROBABILITY * 100)
            {
                rows.add(RowFactory.create(rowId++, null));
            }
            else
            {
                int intVal = rnd.nextInt(1001);
                List<String> list = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int listSize = rnd.nextInt(6);
                    list = new ArrayList<>();
                    for (int j = 0; j < listSize; j++)
                    {
                        list.add(randomString(rnd));
                    }
                }
                rows.add(RowFactory.create(rowId++, RowFactory.create(intVal, list)));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates a DataFrame for tuple&lt;int, set&lt;int&gt;&gt;.
     */
    private Dataset<Row> generateTupleWithSetDataFrame(SparkSession spark, long seed)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(DataTypes.IntegerType), true)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        java.util.Random rnd = new java.util.Random(seed);
        List<Row> rows = new ArrayList<>();
        long rowId = 0;

        for (int i = 0; i < NUM_ROWS; i++)
        {
            if (rnd.nextInt(100) < NULL_PROBABILITY * 100)
            {
                rows.add(RowFactory.create(rowId++, null));
            }
            else
            {
                int intVal = rnd.nextInt(1001);
                List<Integer> setAsList = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int setSize = rnd.nextInt(6);
                    Set<Integer> set = new HashSet<>();
                    for (int j = 0; j < setSize; j++)
                    {
                        set.add(rnd.nextInt(101));
                    }
                    setAsList = new ArrayList<>(set);
                }
                rows.add(RowFactory.create(rowId++, RowFactory.create(intVal, setAsList)));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates a DataFrame for tuple&lt;int, map&lt;text, int&gt;&gt;.
     */
    private Dataset<Row> generateTupleWithMapDataFrame(SparkSession spark, long seed)
    {
        StructType tupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType), true)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", tupleType, true)
        ));

        java.util.Random rnd = new java.util.Random(seed);
        List<Row> rows = new ArrayList<>();
        long rowId = 0;

        for (int i = 0; i < NUM_ROWS; i++)
        {
            if (rnd.nextInt(100) < NULL_PROBABILITY * 100)
            {
                rows.add(RowFactory.create(rowId++, null));
            }
            else
            {
                int intVal = rnd.nextInt(1001);
                Map<String, Integer> map = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int mapSize = rnd.nextInt(5);
                    map = new HashMap<>();
                    for (int j = 0; j < mapSize; j++)
                    {
                        map.put(randomString(rnd), rnd.nextInt(1001));
                    }
                }
                rows.add(RowFactory.create(rowId++, RowFactory.create(intVal, map)));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates a DataFrame for tuple&lt;int, frozen&lt;udt_with_collections&gt;&gt;.
     */
    private Dataset<Row> generateTupleWithUdtDataFrame(SparkSession spark, long seed)
    {
        StructType innerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType udtType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("f1", DataTypes.createArrayType(DataTypes.StringType), true),
            DataTypes.createStructField("f2", DataTypes.createArrayType(DataTypes.StringType), true),
            DataTypes.createStructField("f3", DataTypes.createMapType(DataTypes.IntegerType, DataTypes.StringType), true),
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

        java.util.Random rnd = new java.util.Random(seed);
        List<Row> rows = new ArrayList<>();
        long rowId = 0;

        for (int i = 0; i < NUM_ROWS; i++)
        {
            if (rnd.nextInt(100) < NULL_PROBABILITY * 100)
            {
                rows.add(RowFactory.create(rowId++, null));
            }
            else
            {
                int intVal = rnd.nextInt(1001);

                List<String> list = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int listSize = rnd.nextInt(4);
                    list = new ArrayList<>();
                    for (int j = 0; j < listSize; j++)
                    {
                        list.add(randomString(rnd));
                    }
                }

                Set<String> set = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int setSize = rnd.nextInt(4);
                    set = new HashSet<>();
                    for (int j = 0; j < setSize; j++)
                    {
                        set.add(randomString(rnd));
                    }
                }

                Map<Integer, String> map = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int mapSize = rnd.nextInt(4);
                    map = new HashMap<>();
                    for (int j = 0; j < mapSize; j++)
                    {
                        map.put(rnd.nextInt(101), randomString(rnd));
                    }
                }

                Row innerTuple = RowFactory.create(rnd.nextInt(501), randomString(rnd));
                Row udt = RowFactory.create(list, set == null ? null : new ArrayList<>(set), map, innerTuple);
                Row outerTuple = RowFactory.create(intVal, udt);
                rows.add(RowFactory.create(rowId++, outerTuple));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates a DataFrame for map&lt;frozen&lt;tuple&lt;int, text&gt;&gt;, frozen&lt;tuple&lt;text, int&gt;&gt;&gt;.
     */
    private Dataset<Row> generateMapTupleKeyValueDataFrame(SparkSession spark, long seed)
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

        java.util.Random rnd = new java.util.Random(seed);
        List<Row> rows = new ArrayList<>();
        long rowId = 0;

        for (int i = 0; i < NUM_ROWS; i++)
        {
            if (rnd.nextInt(100) < NULL_PROBABILITY * 100)
            {
                rows.add(RowFactory.create(rowId++, null));
            }
            else
            {
                int mapSize = rnd.nextInt(4);
                Map<Row, Row> map = new HashMap<>();
                for (int j = 0; j < mapSize; j++)
                {
                    Row key = RowFactory.create(rnd.nextInt(501), randomString(rnd));
                    Row value = RowFactory.create(randomString(rnd), rnd.nextInt(501));
                    map.put(key, value);
                }
                rows.add(RowFactory.create(rowId++, map));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates a DataFrame for tuple&lt;int, set&lt;frozen&lt;tuple&lt;text, int&gt;&gt;&gt;&gt;.
     */
    private Dataset<Row> generateTupleSetOfTuplesDataFrame(SparkSession spark, long seed)
    {
        StructType innerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.StringType, false),
            DataTypes.createStructField("_2", DataTypes.IntegerType, false)
        ));

        StructType outerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(innerTupleType), true)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", outerTupleType, true)
        ));

        java.util.Random rnd = new java.util.Random(seed);
        List<Row> rows = new ArrayList<>();
        long rowId = 0;

        for (int i = 0; i < NUM_ROWS; i++)
        {
            if (rnd.nextInt(100) < NULL_PROBABILITY * 100)
            {
                rows.add(RowFactory.create(rowId++, null));
            }
            else
            {
                int intVal = rnd.nextInt(1001);
                List<Row> innerTuples = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int setSize = rnd.nextInt(4);
                    innerTuples = new ArrayList<>();
                    for (int j = 0; j < setSize; j++)
                    {
                        innerTuples.add(RowFactory.create(randomString(rnd), rnd.nextInt(501)));
                    }
                }
                Row outerTuple = RowFactory.create(intVal, innerTuples);
                rows.add(RowFactory.create(rowId++, outerTuple));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Generates a DataFrame for tuple&lt;int, list&lt;frozen&lt;tuple&lt;text, set&lt;int&gt;&gt;&gt;&gt;&gt;.
     */
    private Dataset<Row> generateTupleListOfTuplesDataFrame(SparkSession spark, long seed)
    {
        StructType innerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.StringType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(DataTypes.IntegerType), true)
        ));

        StructType outerTupleType = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("_1", DataTypes.IntegerType, false),
            DataTypes.createStructField("_2", DataTypes.createArrayType(innerTupleType), true)
        ));

        StructType schema = DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("id", DataTypes.LongType, false),
            DataTypes.createStructField("data", outerTupleType, true)
        ));

        java.util.Random rnd = new java.util.Random(seed);
        List<Row> rows = new ArrayList<>();
        long rowId = 0;

        for (int i = 0; i < NUM_ROWS; i++)
        {
            if (rnd.nextInt(100) < NULL_PROBABILITY * 100)
            {
                rows.add(RowFactory.create(rowId++, null));
            }
            else
            {
                int intVal = rnd.nextInt(1001);
                List<Row> innerTuples = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int listSize = rnd.nextInt(4);
                    innerTuples = new ArrayList<>();
                    for (int j = 0; j < listSize; j++)
                    {
                        String str = randomString(rnd);
                        Set<Integer> innerSet = null;
                        if (rnd.nextInt(100) >= 20)
                        {
                            int innerSetSize = rnd.nextInt(4);
                            innerSet = new HashSet<>();
                            for (int k = 0; k < innerSetSize; k++)
                            {
                                innerSet.add(rnd.nextInt(101));
                            }
                        }
                        innerTuples.add(RowFactory.create(str, innerSet == null ? null : new ArrayList<>(innerSet)));
                    }
                }
                Row outerTuple = RowFactory.create(intVal, innerTuples);
                rows.add(RowFactory.create(rowId++, outerTuple));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
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

        // Create UDTs
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TYPE %s.udt_with_collections (f1 list<text>, f2 set<text>, f3 map<int, text>, f4 tuple<int, text>)",
            TEST_KEYSPACE
        ));

        createFixedTupleTables();
    }

    private void createFixedTupleTables()
    {
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

        // Nested tuple
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<tuple<int, frozen<tuple<text, int>>>>)",
            NESTED_TUPLE_TABLE.keyspace(),
            NESTED_TUPLE_TABLE.table()
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
        // Tuple with list of tuples
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
            "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, " +
            "data frozen<tuple<int, list<frozen<tuple<text, set<int>>>>>>)",
            TUPLE_LIST_OF_TUPLES_TABLE.keyspace(),
            TUPLE_LIST_OF_TUPLES_TABLE.table()
        ));
    }

    // ==================== Assertion Context Formatting Methods ====================

    /**
     * Formats a simple tuple&lt;int, text&gt; for display.
     */
    private String formatSimpleTuple(Row tuple)
    {
        if (tuple == null)
        {
            return "NULL";
        }
        return String.format("<%d, '%s'>", tuple.getInt(0), tuple.getString(1));
    }

    /**
     * Formats a tuple test row for display in assertion messages.
     * Row(id, frozen&lt;tuple&lt;int, text&gt;&gt;)
     */
    private String formatTupleRow(Row row)
    {
        if (row == null)
        {
            return "NULL";
        }
        long id = row.getLong(0);
        if (row.isNullAt(1))
        {
            return String.format("Row(id=%d, data=NULL)", id);
        }
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
        if (row == null)
        {
            return "NULL";
        }
        long id = row.getLong(0);
        if (row.isNullAt(1))
        {
            return String.format("Row(id=%d, data=NULL)", id);
        }
        Object data = row.get(1);
        String dataStr = data.toString();
        if (dataStr.length() > 200)
        {
            dataStr = dataStr.substring(0, 197) + "...";
        }
        return String.format("Row(id=%d, data=%s)", id, dataStr);
    }
}
