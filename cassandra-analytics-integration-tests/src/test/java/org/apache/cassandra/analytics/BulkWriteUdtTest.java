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
 * Property-based testing for UDT types.
 */
class BulkWriteUdtTest extends SharedClusterSparkIntegrationTestBase
{
    // Number of rows to test per test method
    private static final int NUM_ROWS = 50;

    // Probability of null values (20% chance of null)
    private static final double NULL_PROBABILITY = 0.2;

    // Minimum guaranteed null rows per batch
    private static final int MIN_NULL_ROWS = 2;

    // Table names
    private static final QualifiedName SIMPLE_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_simple_udt");
    private static final QualifiedName LIST_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_list_udt");
    private static final QualifiedName SET_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_set_udt");
    private static final QualifiedName MAP_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_map_udt");
    private static final QualifiedName UDT_WITH_COLLECTIONS_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_udt_collections");
    private static final QualifiedName DEEPLY_NESTED_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_deep_udt");
    private static final QualifiedName UDT_LIST_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_udt_list_udt");
    private static final QualifiedName UDT_SET_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_udt_set_udt");
    private static final QualifiedName UDT_MAP_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_udt_map_udt");
    private static final QualifiedName TUPLE_OF_UDTS_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_udts");
    private static final QualifiedName UDT_MAP_KEY_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_udt_map_key");
    private static final QualifiedName UDT_WITH_LIST_TUPLE_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_udt_list_tuple");
    private static final QualifiedName UDT_WITH_MAP_TUPLE_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_udt_map_tuple");
    private static final QualifiedName TUPLE_WITH_LIST_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_list_udt");
    private static final QualifiedName TUPLE_WITH_MAP_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_map_udt");
    private static final QualifiedName TUPLE_WITH_SET_UDT_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_tuple_set_udt");
    private static final QualifiedName UDT_WITH_SET_TUPLE_TABLE = new QualifiedName(TEST_KEYSPACE, "qt_udt_set_tuple");

    // ==================== Test Methods ====================

    /**
     * Tests: Simple UDT with text and int fields, including null UDTs
     * <p>Table: CREATE TABLE qt_simple_udt (id BIGINT PRIMARY KEY, data frozen&lt;person&gt;)
     */
    @Test
    void testSimpleUdt()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateSimpleUdtDataFrame(spark, seed);
                truncateTable(SIMPLE_UDT_TABLE);

                bulkWriterDataFrameWriter(sourceData, SIMPLE_UDT_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(SIMPLE_UDT_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

                for (int i = 0; i < sourceRows.size(); i++)
                {
                    Row sourceRow = sourceRows.get(i);
                    Row readRow = readRows.get(i);

                    String context = formatContext(i, formatSimpleUdtRow(sourceRow), formatSimpleUdtRow(readRow));

                    assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

                    if (sourceRow.isNullAt(1))
                    {
                        assertThat(readRow.isNullAt(1)).as(context).isTrue();
                    }
                    else
                    {
                        Row sourceUdt = sourceRow.getStruct(1);
                        Row readUdt = readRow.getStruct(1);

                        assertThat(readUdt.getString(0)).as(context).isEqualTo(sourceUdt.getString(0));
                        assertThat(readUdt.getInt(1)).as(context).isEqualTo(sourceUdt.getInt(1));
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: UDT with list, set, map, and tuple fields
     * <p>Table: CREATE TABLE qt_udt_collections (id BIGINT PRIMARY KEY, data frozen&lt;udt_collections&gt;)
     */
    @Test
    void testUdtWithCollections()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateUdtWithCollectionsDataFrame(spark, seed);
                truncateTable(UDT_WITH_COLLECTIONS_TABLE);

                bulkWriterDataFrameWriter(sourceData, UDT_WITH_COLLECTIONS_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(UDT_WITH_COLLECTIONS_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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
                        Row sourceUdt = sourceRow.getStruct(1);
                        Row readUdt = readRow.getStruct(1);

                        // list field (handle null)
                        if (sourceUdt.isNullAt(0))
                        {
                            assertThat(readUdt.isNullAt(0)).as(context + " (list)").isTrue();
                        }
                        else
                        {
                            assertThat(readUdt.getList(0)).as(context).isEqualTo(sourceUdt.getList(0));
                        }

                        // set field (order doesn't matter, handle null)
                        if (sourceUdt.isNullAt(1))
                        {
                            assertThat(readUdt.isNullAt(1)).as(context + " (set)").isTrue();
                        }
                        else
                        {
                            Set<Integer> sourceSet = new HashSet<>(sourceUdt.getList(1));
                            Set<Integer> readSet = new HashSet<>(readUdt.getList(1));
                            assertThat(readSet).as(context).isEqualTo(sourceSet);
                        }

                        // map field (handle null)
                        if (sourceUdt.isNullAt(2))
                        {
                            assertThat(readUdt.isNullAt(2)).as(context + " (map)").isTrue();
                        }
                        else
                        {
                            assertThat(readUdt.getJavaMap(2)).as(context).isEqualTo(sourceUdt.getJavaMap(2));
                        }

                        // tuple field (handle null)
                        if (sourceUdt.isNullAt(3))
                        {
                            assertThat(readUdt.isNullAt(3)).as(context + " (tuple)").isTrue();
                        }
                        else
                        {
                            Row sourceTuple = sourceUdt.getStruct(3);
                            Row readTuple = readUdt.getStruct(3);
                            assertThat(readTuple.getInt(0)).as(context).isEqualTo(sourceTuple.getInt(0));
                            assertThat(readTuple.getString(1)).as(context).isEqualTo(sourceTuple.getString(1));
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Deeply nested UDT with 3 levels of nesting (level3 contains level2 contains level1)
     * <p>Table: CREATE TABLE qt_deep_udt (id BIGINT PRIMARY KEY, data frozen&lt;level3&gt;)
     */
    @Test
    void testDeeplyNestedUdt()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateDeeplyNestedUdtDataFrame(spark, seed);
                truncateTable(DEEPLY_NESTED_UDT_TABLE);

                bulkWriterDataFrameWriter(sourceData, DEEPLY_NESTED_UDT_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(DEEPLY_NESTED_UDT_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

                for (int i = 0; i < sourceRows.size(); i++)
                {
                    Row sourceRow = sourceRows.get(i);
                    Row readRow = readRows.get(i);

                    String context = formatContext(i, formatDeeplyNestedUdtRow(sourceRow), formatDeeplyNestedUdtRow(readRow));

                    assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

                    if (sourceRow.isNullAt(1))
                    {
                        assertThat(readRow.isNullAt(1)).as(context).isTrue();
                    }
                    else
                    {
                        Row sourceLevel3 = sourceRow.getStruct(1);
                        Row readLevel3 = readRow.getStruct(1);

                        // level3.field3
                        assertThat(readLevel3.getLong(1)).as(context).isEqualTo(sourceLevel3.getLong(1));

                        // Handle null level2 (level3.nested)
                        if (sourceLevel3.isNullAt(0))
                        {
                            assertThat(readLevel3.isNullAt(0)).as(context + " (level2)").isTrue();
                        }
                        else
                        {
                            Row sourceLevel2 = sourceLevel3.getStruct(0);
                            Row readLevel2 = readLevel3.getStruct(0);

                            // level2.field2
                            assertThat(readLevel2.getInt(1)).as(context).isEqualTo(sourceLevel2.getInt(1));

                            // Handle null level1 (level2.nested)
                            if (sourceLevel2.isNullAt(0))
                            {
                                assertThat(readLevel2.isNullAt(0)).as(context + " (level1)").isTrue();
                            }
                            else
                            {
                                Row sourceLevel1 = sourceLevel2.getStruct(0);
                                Row readLevel1 = readLevel2.getStruct(0);

                                assertThat(readLevel1.getString(0)).as(context).isEqualTo(sourceLevel1.getString(0));
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: List collection containing UDT elements
     * <p>Table: CREATE TABLE qt_list_udt (id BIGINT PRIMARY KEY, data list&lt;frozen&lt;person&gt;&gt;)
     */
    @Test
    void testListOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateListUdtDataFrame(spark, seed);
                truncateTable(LIST_UDT_TABLE);

                bulkWriterDataFrameWriter(sourceData, LIST_UDT_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(LIST_UDT_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

                for (int i = 0; i < sourceRows.size(); i++)
                {
                    Row sourceRow = sourceRows.get(i);
                    Row readRow = readRows.get(i);

                    String context = formatContext(i, formatListUdtRow(sourceRow), formatListUdtRow(readRow));

                    assertThat(readRow.getLong(0)).as(context).isEqualTo(sourceRow.getLong(0));

                    if (sourceRow.isNullAt(1))
                    {
                        assertThat(readRow.isNullAt(1)).as(context).isTrue();
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

                            assertThat(readList).as(context).hasSize(sourceList.size());

                            for (int j = 0; j < sourceList.size(); j++)
                            {
                                Row sourceUdt = sourceList.get(j);
                                Row readUdt = readList.get(j);
                                String itemContext = context + String.format("\n  Item[%d]: source=%s, read=%s",
                                                                             j, formatPersonUdt(sourceUdt), formatPersonUdt(readUdt));

                                assertThat(readUdt.getString(0)).as(itemContext).isEqualTo(sourceUdt.getString(0));
                                assertThat(readUdt.getInt(1)).as(itemContext).isEqualTo(sourceUdt.getInt(1));
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Set collection containing UDT elements with order-independent comparison
     * <p>Table: CREATE TABLE qt_set_udt (id BIGINT PRIMARY KEY, data set&lt;frozen&lt;person&gt;&gt;)
     */
    @Test
    void testSetOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateSetUdtDataFrame(spark, seed);
                truncateTable(SET_UDT_TABLE);

                bulkWriterDataFrameWriter(sourceData, SET_UDT_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(SET_UDT_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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
                        List<Row> sourceList = sourceRow.getList(1);
                        // Cassandra stores empty collections as NULL
                        if (sourceList.isEmpty())
                        {
                            assertThat(readRow.isNullAt(1)).as(context).isTrue();
                        }
                        else
                        {
                            List<Row> readList = readRow.getList(1);

                            assertThat(readList).as(context).hasSize(sourceList.size());

                            // Convert to sets for comparison (order doesn't matter)
                            Set<String> sourceSet = sourceList.stream()
                                                              .map(r -> r.getString(0) + ":" + r.getInt(1))
                                                              .collect(Collectors.toSet());
                            Set<String> readSet = readList.stream()
                                                          .map(r -> r.getString(0) + ":" + r.getInt(1))
                                                          .collect(Collectors.toSet());

                            assertThat(readSet).as(context).isEqualTo(sourceSet);
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: UDT used as map value with text keys
     * <p>Table: CREATE TABLE qt_map_udt (id BIGINT PRIMARY KEY, data map&lt;text, frozen&lt;person&gt;&gt;)
     */
    @Test
    void testUdtAsMapValue()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateMapUdtDataFrame(spark, seed);
                truncateTable(MAP_UDT_TABLE);

                bulkWriterDataFrameWriter(sourceData, MAP_UDT_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(MAP_UDT_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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
                        Map<String, Row> sourceMap = sourceRow.getJavaMap(1);
                        // Cassandra stores empty collections as NULL
                        if (sourceMap.isEmpty())
                        {
                            assertThat(readRow.isNullAt(1)).as(context).isTrue();
                        }
                        else
                        {
                            Map<String, Row> readMap = readRow.getJavaMap(1);
                            assertThat(readMap).as(context).hasSize(sourceMap.size());

                            for (String key : sourceMap.keySet())
                            {
                                assertThat(readMap).as(context + String.format("\n  Missing key: '%s'", key)).containsKey(key);
                                Row sourceUdt = sourceMap.get(key);
                                Row readUdt = readMap.get(key);
                                String udtContext = context + String.format("\n  Key['%s']: source=%s, read=%s",
                                                                            key, formatPersonUdt(sourceUdt), formatPersonUdt(readUdt));

                                assertThat(readUdt.getString(0)).as(udtContext).isEqualTo(sourceUdt.getString(0));
                                assertThat(readUdt.getInt(1)).as(udtContext).isEqualTo(sourceUdt.getInt(1));
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Tuple containing two different UDT types (person and address)
     * <p>Table: CREATE TABLE qt_tuple_udts (id BIGINT PRIMARY KEY, data tuple&lt;frozen&lt;person&gt;, frozen&lt;address&gt;&gt;)
     */
    @Test
    void testTupleOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateTupleOfUdtsDataFrame(spark, seed);
                truncateTable(TUPLE_OF_UDTS_TABLE);

                bulkWriterDataFrameWriter(sourceData, TUPLE_OF_UDTS_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(TUPLE_OF_UDTS_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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

                        Row sourcePerson = sourceTuple.getStruct(0);
                        Row readPerson = readTuple.getStruct(0);
                        assertThat(readPerson.getString(0)).as(context).isEqualTo(sourcePerson.getString(0));
                        assertThat(readPerson.getInt(1)).as(context).isEqualTo(sourcePerson.getInt(1));

                        Row sourceAddress = sourceTuple.getStruct(1);
                        Row readAddress = readTuple.getStruct(1);
                        assertThat(readAddress.getString(0)).as(context).isEqualTo(sourceAddress.getString(0));
                        assertThat(readAddress.getInt(1)).as(context).isEqualTo(sourceAddress.getInt(1));
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: UDT used as map key with text values
     * <p>Table: CREATE TABLE qt_udt_map_key (id BIGINT PRIMARY KEY, data map&lt;frozen&lt;person&gt;, text&gt;)
     */
    @Test
    void testUdtAsMapKey()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateUdtMapKeyDataFrame(spark, seed);
                truncateTable(UDT_MAP_KEY_TABLE);

                bulkWriterDataFrameWriter(sourceData, UDT_MAP_KEY_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(UDT_MAP_KEY_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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
                        Map<Row, String> sourceMap = sourceRow.getJavaMap(1);
                        // Cassandra stores empty collections as NULL
                        if (sourceMap.isEmpty())
                        {
                            assertThat(readRow.isNullAt(1)).as(context).isTrue();
                        }
                        else
                        {
                            Map<Row, String> readMap = readRow.getJavaMap(1);
                            assertThat(readMap).as(context).hasSize(sourceMap.size());

                            // Compare by converting to string representation
                            Map<String, String> sourceStringMap = new HashMap<>();
                            Map<String, String> readStringMap = new HashMap<>();
                            for (Map.Entry<Row, String> entry : sourceMap.entrySet())
                            {
                                sourceStringMap.put(entry.getKey().getString(0) + ":" + entry.getKey().getInt(1), entry.getValue());
                            }
                            for (Map.Entry<Row, String> entry : readMap.entrySet())
                            {
                                readStringMap.put(entry.getKey().getString(0) + ":" + entry.getKey().getInt(1), entry.getValue());
                            }
                            assertThat(readStringMap).as(context).isEqualTo(sourceStringMap);
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: UDT containing a list of tuples
     * <p>Table: CREATE TABLE qt_udt_list_tuple (id BIGINT PRIMARY KEY, data frozen&lt;udt_list_tuple&gt;)
     */
    @Test
    void testUdtWithListOfTuples()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateUdtWithListTupleDataFrame(spark, seed);
                truncateTable(UDT_WITH_LIST_TUPLE_TABLE);

                bulkWriterDataFrameWriter(sourceData, UDT_WITH_LIST_TUPLE_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(UDT_WITH_LIST_TUPLE_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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
                        Row sourceUdt = sourceRow.getStruct(1);
                        Row readUdt = readRow.getStruct(1);

                        // Handle null list within UDT
                        if (sourceUdt.isNullAt(0))
                        {
                            assertThat(readUdt.isNullAt(0)).as(context + " (list)").isTrue();
                        }
                        else
                        {
                            List<Row> sourceList = sourceUdt.getList(0);
                            // For frozen UDTs, empty collections may stay as empty (not become null)
                            if (sourceList.isEmpty())
                            {
                                if (!readUdt.isNullAt(0))
                                {
                                    List<Row> readList = readUdt.getList(0);
                                    assertThat(readList).as(context + " (empty list)").isEmpty();
                                }
                            }
                            else
                            {
                                List<Row> readList = readUdt.getList(0);

                                assertThat(readList).as(context).hasSize(sourceList.size());

                                for (int j = 0; j < sourceList.size(); j++)
                                {
                                    Row sourceTuple = sourceList.get(j);
                                    Row readTuple = readList.get(j);
                                    String tupleContext = context + String.format("\n  Tuple[%d]: source=<%d, '%s'>, read=<%d, '%s'>",
                                                                                  j, sourceTuple.getInt(0), sourceTuple.getString(1), readTuple.getInt(0),
                                                                                  readTuple.getString(1));

                                    assertThat(readTuple.getInt(0)).as(tupleContext).isEqualTo(sourceTuple.getInt(0));
                                    assertThat(readTuple.getString(1)).as(tupleContext).isEqualTo(sourceTuple.getString(1));
                                }
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: UDT containing a set of tuples with order-independent comparison
     * <p>Table: CREATE TABLE qt_udt_set_tuple (id BIGINT PRIMARY KEY, data frozen&lt;udt_set_tuple&gt;)
     */
    @Test
    void testUdtWithSetOfTuples()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateUdtWithSetTupleDataFrame(spark, seed);
                truncateTable(UDT_WITH_SET_TUPLE_TABLE);

                bulkWriterDataFrameWriter(sourceData, UDT_WITH_SET_TUPLE_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(UDT_WITH_SET_TUPLE_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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
                        Row sourceUdt = sourceRow.getStruct(1);
                        Row readUdt = readRow.getStruct(1);

                        // Handle null set within UDT
                        if (sourceUdt.isNullAt(0))
                        {
                            assertThat(readUdt.isNullAt(0)).as(context + " (set)").isTrue();
                        }
                        else
                        {
                            List<Row> sourceList = sourceUdt.getList(0);
                            // For frozen UDTs, empty collections may stay as empty (not become null)
                            if (sourceList.isEmpty())
                            {
                                if (!readUdt.isNullAt(0))
                                {
                                    List<Row> readList = readUdt.getList(0);
                                    assertThat(readList).as(context + " (empty set)").isEmpty();
                                }
                            }
                            else
                            {
                                List<Row> readList = readUdt.getList(0);

                                assertThat(readList).as(context).hasSize(sourceList.size());

                                // Convert to sets for comparison (order doesn't matter)
                                Set<String> sourceSet = sourceList.stream()
                                                                  .map(r -> r.getInt(0) + ":" + r.getString(1))
                                                                  .collect(Collectors.toSet());
                                Set<String> readSet = readList.stream()
                                                              .map(r -> r.getInt(0) + ":" + r.getString(1))
                                                              .collect(Collectors.toSet());

                                assertThat(readSet).as(context).isEqualTo(sourceSet);
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: UDT containing a map with text keys and tuple values
     * <p>Table: CREATE TABLE qt_udt_map_tuple (id BIGINT PRIMARY KEY, data frozen&lt;udt_map_tuple&gt;)
     */
    @Test
    void testUdtWithMapOfTuples()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateUdtWithMapTupleDataFrame(spark, seed);
                truncateTable(UDT_WITH_MAP_TUPLE_TABLE);

                bulkWriterDataFrameWriter(sourceData, UDT_WITH_MAP_TUPLE_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(UDT_WITH_MAP_TUPLE_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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
                        Row sourceUdt = sourceRow.getStruct(1);
                        Row readUdt = readRow.getStruct(1);

                        // Handle null map within UDT
                        if (sourceUdt.isNullAt(0))
                        {
                            assertThat(readUdt.isNullAt(0)).as(context + " (map)").isTrue();
                        }
                        else
                        {
                            Map<String, Row> sourceMap = sourceUdt.getJavaMap(0);
                            // For frozen UDTs, empty collections may stay as empty (not become null)
                            if (sourceMap.isEmpty())
                            {
                                if (!readUdt.isNullAt(0))
                                {
                                    Map<String, Row> readMap = readUdt.getJavaMap(0);
                                    assertThat(readMap).as(context + " (empty map)").isEmpty();
                                }
                            }
                            else
                            {
                                Map<String, Row> readMap = readUdt.getJavaMap(0);

                                assertThat(readMap).as(context).hasSize(sourceMap.size());

                                for (String key : sourceMap.keySet())
                                {
                                    assertThat(readMap).as(context + String.format("\n  Missing key: '%s'", key)).containsKey(key);
                                    Row sourceTuple = sourceMap.get(key);
                                    Row readTuple = readMap.get(key);
                                    String tupleContext = context + String.format("\n  Key['%s']: source=<%d, '%s'>, read=<%d, '%s'>",
                                                                                  key, sourceTuple.getInt(0), sourceTuple.getString(1), readTuple.getInt(0),
                                                                                  readTuple.getString(1));

                                    assertThat(readTuple.getInt(0)).as(tupleContext).isEqualTo(sourceTuple.getInt(0));
                                    assertThat(readTuple.getString(1)).as(tupleContext).isEqualTo(sourceTuple.getString(1));
                                }
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Tuple containing an int and a list of UDTs
     * <p>Table: CREATE TABLE qt_tuple_list_udt (id BIGINT PRIMARY KEY, data tuple&lt;int, list&lt;frozen&lt;person&gt;&gt;&gt;)
     */
    @Test
    void testTupleWithListOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateTupleWithListUdtDataFrame(spark, seed);
                truncateTable(TUPLE_WITH_LIST_UDT_TABLE);

                bulkWriterDataFrameWriter(sourceData, TUPLE_WITH_LIST_UDT_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(TUPLE_WITH_LIST_UDT_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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

                        // Handle null list within tuple
                        if (sourceTuple.isNullAt(1))
                        {
                            assertThat(readTuple.isNullAt(1)).as(context + " (list)").isTrue();
                        }
                        else
                        {
                            List<Row> sourceList = sourceTuple.getList(1);
                            // For tuples, empty collections may stay as empty (not become null)
                            if (sourceList.isEmpty())
                            {
                                if (!readTuple.isNullAt(1))
                                {
                                    List<Row> readList = readTuple.getList(1);
                                    assertThat(readList).as(context + " (empty list)").isEmpty();
                                }
                            }
                            else
                            {
                                List<Row> readList = readTuple.getList(1);

                                assertThat(readList).as(context).hasSize(sourceList.size());

                                for (int j = 0; j < sourceList.size(); j++)
                                {
                                    Row sourceUdt = sourceList.get(j);
                                    Row readUdt = readList.get(j);
                                    String udtContext = context + String.format("\n  UDT[%d]: source=%s, read=%s",
                                                                                j, formatPersonUdt(sourceUdt), formatPersonUdt(readUdt));

                                    assertThat(readUdt.getString(0)).as(udtContext).isEqualTo(sourceUdt.getString(0));
                                    assertThat(readUdt.getInt(1)).as(udtContext).isEqualTo(sourceUdt.getInt(1));
                                }
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Tuple containing text and a map with UDT values
     * <p>Table: CREATE TABLE qt_tuple_map_udt (id BIGINT PRIMARY KEY, data tuple&lt;text, map&lt;text, frozen&lt;person&gt;&gt;&gt;)
     */
    @Test
    void testTupleWithMapOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateTupleWithMapUdtDataFrame(spark, seed);
                truncateTable(TUPLE_WITH_MAP_UDT_TABLE);

                bulkWriterDataFrameWriter(sourceData, TUPLE_WITH_MAP_UDT_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(TUPLE_WITH_MAP_UDT_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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

                        assertThat(readTuple.getString(0)).as(context).isEqualTo(sourceTuple.getString(0));

                        // Handle null map within tuple
                        if (sourceTuple.isNullAt(1))
                        {
                            assertThat(readTuple.isNullAt(1)).as(context + " (map)").isTrue();
                        }
                        else
                        {
                            Map<String, Row> sourceMap = sourceTuple.getJavaMap(1);
                            // For frozen tuples, empty collections may stay as empty (not become null)
                            if (sourceMap.isEmpty())
                            {
                                if (!readTuple.isNullAt(1))
                                {
                                    Map<String, Row> readMap = readTuple.getJavaMap(1);
                                    assertThat(readMap).as(context + " (empty map)").isEmpty();
                                }
                            }
                            else
                            {
                                Map<String, Row> readMap = readTuple.getJavaMap(1);

                                assertThat(readMap).as(context).hasSize(sourceMap.size());

                                for (String key : sourceMap.keySet())
                                {
                                    assertThat(readMap).as(context + String.format("\n  Missing key: '%s'", key)).containsKey(key);
                                    Row sourceUdt = sourceMap.get(key);
                                    Row readUdt = readMap.get(key);
                                    String udtContext = context + String.format("\n  Key['%s']: source=%s, read=%s",
                                                                                key, formatPersonUdt(sourceUdt), formatPersonUdt(readUdt));

                                    assertThat(readUdt.getString(0)).as(udtContext).isEqualTo(sourceUdt.getString(0));
                                    assertThat(readUdt.getInt(1)).as(udtContext).isEqualTo(sourceUdt.getInt(1));
                                }
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: Tuple containing int and a set of UDTs with order-independent comparison
     * <p>Table: CREATE TABLE qt_tuple_set_udt (id BIGINT PRIMARY KEY, data tuple&lt;int, set&lt;frozen&lt;person&gt;&gt;&gt;)
     */
    @Test
    void testTupleWithSetOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateTupleWithSetUdtDataFrame(spark, seed);
                truncateTable(TUPLE_WITH_SET_UDT_TABLE);

                bulkWriterDataFrameWriter(sourceData, TUPLE_WITH_SET_UDT_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(TUPLE_WITH_SET_UDT_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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

                        // Handle null set within tuple
                        if (sourceTuple.isNullAt(1))
                        {
                            assertThat(readTuple.isNullAt(1)).as(context + " (set)").isTrue();
                        }
                        else
                        {
                            List<Row> sourceList = sourceTuple.getList(1);
                            // For frozen tuples, empty collections may stay as empty (not become null)
                            if (sourceList.isEmpty())
                            {
                                if (!readTuple.isNullAt(1))
                                {
                                    List<Row> readList = readTuple.getList(1);
                                    assertThat(readList).as(context + " (empty set)").isEmpty();
                                }
                            }
                            else
                            {
                                List<Row> readList = readTuple.getList(1);

                                assertThat(readList).as(context).hasSize(sourceList.size());

                                // Convert to sets for comparison (order doesn't matter)
                                Set<String> sourceSet = sourceList.stream()
                                                                  .map(r -> r.getString(0) + ":" + r.getInt(1))
                                                                  .collect(Collectors.toSet());
                                Set<String> readSet = readList.stream()
                                                              .map(r -> r.getString(0) + ":" + r.getInt(1))
                                                              .collect(Collectors.toSet());

                                assertThat(readSet).as(context).isEqualTo(sourceSet);
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: UDT containing a list of nested UDTs
     * <p>Table: CREATE TABLE qt_udt_list_udt (id BIGINT PRIMARY KEY, data frozen&lt;udt_container&gt;)
     */
    @Test
    void testUdtWithListOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateUdtWithListUdtDataFrame(spark, seed);
                truncateTable(UDT_LIST_UDT_TABLE);

                bulkWriterDataFrameWriter(sourceData, UDT_LIST_UDT_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(UDT_LIST_UDT_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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
                        Row sourceUdt = sourceRow.getStruct(1);
                        Row readUdt = readRow.getStruct(1);

                        // Handle null inner list within UDT
                        if (sourceUdt.isNullAt(0))
                        {
                            assertThat(readUdt.isNullAt(0)).as(context + " (inner)").isTrue();
                        }
                        else
                        {
                            List<Row> sourceInner = sourceUdt.getList(0);
                            // For frozen UDTs, empty collections may stay as empty (not become null)
                            if (sourceInner.isEmpty())
                            {
                                if (!readUdt.isNullAt(0))
                                {
                                    List<Row> readInner = readUdt.getList(0);
                                    assertThat(readInner).as(context + " (empty inner)").isEmpty();
                                }
                            }
                            else
                            {
                                List<Row> readInner = readUdt.getList(0);

                                assertThat(readInner).as(context).hasSize(sourceInner.size());

                                for (int j = 0; j < sourceInner.size(); j++)
                                {
                                    Row sourceInnerUdt = sourceInner.get(j);
                                    Row readInnerUdt = readInner.get(j);
                                    String innerContext = context + String.format("\n  Inner[%d]: source=%s, read=%s",
                                                                                  j, formatPersonUdt(sourceInnerUdt), formatPersonUdt(readInnerUdt));

                                    assertThat(readInnerUdt.getString(0)).as(innerContext).isEqualTo(sourceInnerUdt.getString(0));
                                    assertThat(readInnerUdt.getInt(1)).as(innerContext).isEqualTo(sourceInnerUdt.getInt(1));
                                }
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: UDT containing a set of nested UDTs with order-independent comparison
     * <p>Table: CREATE TABLE qt_udt_set_udt (id BIGINT PRIMARY KEY, data frozen&lt;udt_set_container&gt;)
     */
    @Test
    void testUdtWithSetOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateUdtWithSetUdtDataFrame(spark, seed);
                truncateTable(UDT_SET_UDT_TABLE);

                bulkWriterDataFrameWriter(sourceData, UDT_SET_UDT_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(UDT_SET_UDT_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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
                        Row sourceUdt = sourceRow.getStruct(1);
                        Row readUdt = readRow.getStruct(1);

                        // Handle null set within UDT
                        if (sourceUdt.isNullAt(0))
                        {
                            assertThat(readUdt.isNullAt(0)).as(context + " (set)").isTrue();
                        }
                        else
                        {
                            List<Row> sourceList = sourceUdt.getList(0);
                            // For frozen UDTs, empty collections may stay as empty (not become null)
                            if (sourceList.isEmpty())
                            {
                                if (!readUdt.isNullAt(0))
                                {
                                    List<Row> readList = readUdt.getList(0);
                                    assertThat(readList).as(context + " (empty set)").isEmpty();
                                }
                            }
                            else
                            {
                                List<Row> readList = readUdt.getList(0);

                                assertThat(readList).as(context).hasSize(sourceList.size());

                                // Convert to sets for comparison (order doesn't matter)
                                Set<String> sourceSet = sourceList.stream()
                                                                  .map(r -> r.getString(0) + ":" + r.getInt(1))
                                                                  .collect(Collectors.toSet());
                                Set<String> readSet = readList.stream()
                                                              .map(r -> r.getString(0) + ":" + r.getInt(1))
                                                              .collect(Collectors.toSet());

                                assertThat(readSet).as(context).isEqualTo(sourceSet);
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }

    /**
     * Tests: UDT containing a map with UDT keys and UDT values
     * <p>Table: CREATE TABLE qt_udt_map_udt (id BIGINT PRIMARY KEY, data frozen&lt;udt_map_container&gt;)
     */
    @Test
    void testUdtWithMapOfUdts()
    {
        SparkSession spark = getOrCreateSparkSession();

        qt().withExamples(1)
            .forAll(integers().all())
            .checkAssert(seed -> {
                Dataset<Row> sourceData = generateUdtWithMapUdtDataFrame(spark, seed);
                truncateTable(UDT_MAP_UDT_TABLE);

                bulkWriterDataFrameWriter(sourceData, UDT_MAP_UDT_TABLE).save();
                Dataset<Row> readData = bulkReaderDataFrame(UDT_MAP_UDT_TABLE).load();

                List<Row> sourceRows = sourceData.sort("id").collectAsList();
                List<Row> readRows = readData.sort("id").collectAsList();

                assertThat(readRows).hasSize(sourceRows.size());

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
                        Row sourceUdt = sourceRow.getStruct(1);
                        Row readUdt = readRow.getStruct(1);

                        // Handle null map within UDT
                        if (sourceUdt.isNullAt(0))
                        {
                            assertThat(readUdt.isNullAt(0)).as(context + " (map)").isTrue();
                        }
                        else
                        {
                            Map<Row, Row> sourceMap = sourceUdt.getJavaMap(0);
                            // For frozen UDTs, empty collections may stay as empty (not become null)
                            if (sourceMap.isEmpty())
                            {
                                if (!readUdt.isNullAt(0))
                                {
                                    Map<Row, Row> readMap = readUdt.getJavaMap(0);
                                    assertThat(readMap).as(context + " (empty map)").isEmpty();
                                }
                            }
                            else
                            {
                                Map<Row, Row> readMap = readUdt.getJavaMap(0);

                                assertThat(readMap).as(context).hasSize(sourceMap.size());

                                // Compare by converting to string representation
                                Map<String, String> sourceStringMap = new HashMap<>();
                                Map<String, String> readStringMap = new HashMap<>();
                                for (Map.Entry<Row, Row> entry : sourceMap.entrySet())
                                {
                                    String personKey = entry.getKey().getString(0) + ":" + entry.getKey().getInt(1);
                                    String addressValue = entry.getValue().getString(0) + ":" + entry.getValue().getInt(1);
                                    sourceStringMap.put(personKey, addressValue);
                                }
                                for (Map.Entry<Row, Row> entry : readMap.entrySet())
                                {
                                    String personKey = entry.getKey().getString(0) + ":" + entry.getKey().getInt(1);
                                    String addressValue = entry.getValue().getString(0) + ":" + entry.getValue().getInt(1);
                                    readStringMap.put(personKey, addressValue);
                                }
                                assertThat(readStringMap).as(context).isEqualTo(sourceStringMap);
                            }
                        }
                    }
                }

                sourceData.unpersist();
                readData.unpersist();
            });
    }


    @Override
    protected void initializeSchemaForTest()
    {
        // Create the keyspace first
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);

        // Simple UDT: person(name text, age int)
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.person (name text, age int)",
        TEST_KEYSPACE));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<person>)",
        TEST_KEYSPACE, SIMPLE_UDT_TABLE.table()));

        // Nested UDT: address(street text, number int)
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.address (street text, number int)",
        TEST_KEYSPACE));

        // List of UDTs: list<frozen<person>>
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data list<frozen<person>>)",
        TEST_KEYSPACE, LIST_UDT_TABLE.table()));

        // Set of UDTs: set<frozen<person>>
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data set<frozen<person>>)",
        TEST_KEYSPACE, SET_UDT_TABLE.table()));

        // Map of UDTs: map<text, frozen<person>>
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data map<text, frozen<person>>)",
        TEST_KEYSPACE, MAP_UDT_TABLE.table()));

        // UDT with all collections
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.udt_collections (list_field list<text>, set_field set<int>, map_field map<text, int>, tuple_field tuple<int, text>)",
        TEST_KEYSPACE));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<udt_collections>)",
        TEST_KEYSPACE, UDT_WITH_COLLECTIONS_TABLE.table()));

        // Deeply nested UDT (3 levels)
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.level1 (field text)",
        TEST_KEYSPACE));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.level2 (nested frozen<level1>, field2 int)",
        TEST_KEYSPACE));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.level3 (nested frozen<level2>, field3 bigint)",
        TEST_KEYSPACE));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<level3>)",
        TEST_KEYSPACE, DEEPLY_NESTED_UDT_TABLE.table()));

        createAdvancedUdtTables();
    }

    private void createAdvancedUdtTables()
    {
        // UDT with list of UDTs: udt_container(inner list<frozen<person>>)
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.udt_container (inner list<frozen<person>>)",
        TEST_KEYSPACE));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<udt_container>)",
        TEST_KEYSPACE, UDT_LIST_UDT_TABLE.table()));

        // UDT with set of UDTs: udt_set_container(inner set<frozen<person>>)
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.udt_set_container (inner set<frozen<person>>)",
        TEST_KEYSPACE));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<udt_set_container>)",
        TEST_KEYSPACE, UDT_SET_UDT_TABLE.table()));

        // UDT with map of UDTs: udt_map_container(inner map<frozen<person>, frozen<address>>)
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.udt_map_container (inner map<frozen<person>, frozen<address>>)",
        TEST_KEYSPACE));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<udt_map_container>)",
        TEST_KEYSPACE, UDT_MAP_UDT_TABLE.table()));

        // Tuple of UDTs: tuple<frozen<person>, frozen<address>>
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data tuple<frozen<person>, frozen<address>>)",
        TEST_KEYSPACE, TUPLE_OF_UDTS_TABLE.table()));

        // UDT as map key: map<frozen<person>, text>
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data map<frozen<person>, text>)",
        TEST_KEYSPACE, UDT_MAP_KEY_TABLE.table()));

        // UDT with list of tuples: udt_list_tuple(items list<tuple<int, text>>)
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.udt_list_tuple (items list<tuple<int, text>>)",
        TEST_KEYSPACE));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<udt_list_tuple>)",
        TEST_KEYSPACE, UDT_WITH_LIST_TUPLE_TABLE.table()));

        // UDT with map of tuples: udt_map_tuple(items map<text, tuple<int, text>>)
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.udt_map_tuple (items map<text, tuple<int, text>>)",
        TEST_KEYSPACE));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<udt_map_tuple>)",
        TEST_KEYSPACE, UDT_WITH_MAP_TUPLE_TABLE.table()));

        // Tuple with list of UDTs: tuple<int, list<frozen<person>>>
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data tuple<int, list<frozen<person>>>)",
        TEST_KEYSPACE, TUPLE_WITH_LIST_UDT_TABLE.table()));

        // Tuple with map of UDTs: tuple<text, map<text, frozen<person>>>
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data tuple<text, map<text, frozen<person>>>)",
        TEST_KEYSPACE, TUPLE_WITH_MAP_UDT_TABLE.table()));

        // Tuple with set of UDTs: tuple<int, set<frozen<person>>>
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data tuple<int, set<frozen<person>>>)",
        TEST_KEYSPACE, TUPLE_WITH_SET_UDT_TABLE.table()));

        // UDT with set of tuples: udt_set_tuple(items set<tuple<int, text>>)
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TYPE %s.udt_set_tuple (items set<tuple<int, text>>)",
        TEST_KEYSPACE));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY, data frozen<udt_set_tuple>)",
        TEST_KEYSPACE, UDT_WITH_SET_TUPLE_TABLE.table()));
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration().nodesPerDc(3);
    }

    // ==================== Utility Methods ====================

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

    /**
     * Truncates a table to remove stale data from previous test runs
     */
    private void truncateTable(QualifiedName tableName)
    {
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "TRUNCATE %s.%s",
        TEST_KEYSPACE, tableName.table()));
    }

    // ==================== DataFrame Generation Methods ====================

    private Dataset<Row> generateSimpleUdtDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", personType, true)
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
                String name = randomString(rnd);
                int age = rnd.nextInt(101);
                Row udt = RowFactory.create(name, age);
                rows.add(RowFactory.create(rowId++, udt));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateListUdtDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", DataTypes.createArrayType(personType, false), true)
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
                List<Row> persons = new ArrayList<>();
                for (int j = 0; j < listSize; j++)
                {
                    persons.add(RowFactory.create(randomString(rnd), rnd.nextInt(101)));
                }
                rows.add(RowFactory.create(rowId++, persons));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateSetUdtDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", DataTypes.createArrayType(personType, false), true)
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
                List<Row> persons = new ArrayList<>();
                for (int j = 0; j < setSize; j++)
                {
                    persons.add(RowFactory.create(randomString(rnd), rnd.nextInt(101)));
                }
                rows.add(RowFactory.create(rowId++, persons));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateMapUdtDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", DataTypes.createMapType(DataTypes.StringType, personType, false), true)
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
                int mapSize = rnd.nextInt(6);
                Map<String, Row> personMap = new HashMap<>();
                for (int j = 0; j < mapSize; j++)
                {
                    String key = randomString(rnd);
                    Row person = RowFactory.create(randomString(rnd), rnd.nextInt(101));
                    personMap.put(key, person);
                }
                rows.add(RowFactory.create(rowId++, personMap));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateUdtWithCollectionsDataFrame(SparkSession spark, long seed)
    {
        StructType tupleType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("_1", DataTypes.IntegerType, false),
        DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType udtCollectionsType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("list_field", DataTypes.createArrayType(DataTypes.StringType, false), true),
        DataTypes.createStructField("set_field", DataTypes.createArrayType(DataTypes.IntegerType, false), true),
        DataTypes.createStructField("map_field", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false), true),
        DataTypes.createStructField("tuple_field", tupleType, true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", udtCollectionsType, true)
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
                // Generate random list (0-3 items) or null
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

                // Generate random set (0-3 items) or null
                List<Integer> set = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int setSize = rnd.nextInt(4);
                    Set<Integer> setData = new HashSet<>();
                    for (int j = 0; j < setSize; j++)
                    {
                        setData.add(rnd.nextInt(101));
                    }
                    set = new ArrayList<>(setData);
                }

                // Generate random map (0-3 items) or null
                Map<String, Integer> map = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int mapSize = rnd.nextInt(4);
                    map = new HashMap<>();
                    for (int j = 0; j < mapSize; j++)
                    {
                        map.put(randomString(rnd), rnd.nextInt(101));
                    }
                }

                // Generate tuple (80% non-null, 20% null)
                Row tuple = null;
                if (rnd.nextInt(100) >= 20)
                {
                    tuple = RowFactory.create(rnd.nextInt(101), randomString(rnd));
                }

                Row udt = RowFactory.create(list, set, map, tuple);
                rows.add(RowFactory.create(rowId++, udt));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateDeeplyNestedUdtDataFrame(SparkSession spark, long seed)
    {
        StructType level1Type = DataTypes.createStructType(List.of(
        DataTypes.createStructField("field", DataTypes.StringType, true)
        ));

        StructType level2Type = DataTypes.createStructType(List.of(
        DataTypes.createStructField("nested", level1Type, true),
        DataTypes.createStructField("field2", DataTypes.IntegerType, true)
        ));

        StructType level3Type = DataTypes.createStructType(List.of(
        DataTypes.createStructField("nested", level2Type, true),
        DataTypes.createStructField("field3", DataTypes.LongType, true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", level3Type, true)
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
                String level1Field = randomString(rnd);
                int level2Field = rnd.nextInt(101);
                long level3Field = rnd.nextInt(1001);
                boolean hasLevel1 = rnd.nextInt(100) >= 20;
                boolean hasLevel2 = rnd.nextInt(100) >= 20;

                Row level1 = hasLevel1 ? RowFactory.create(level1Field) : null;
                Row level2 = hasLevel2 ? RowFactory.create(level1, level2Field) : null;
                Row level3 = RowFactory.create(level2, level3Field);
                rows.add(RowFactory.create(rowId++, level3));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateTupleOfUdtsDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType addressType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("street", DataTypes.StringType, true),
        DataTypes.createStructField("number", DataTypes.IntegerType, true)
        ));

        StructType tupleType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("_1", personType, false),
        DataTypes.createStructField("_2", addressType, false)
        ));

        StructType schema = DataTypes.createStructType(List.of(
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
                Row person = RowFactory.create(randomString(rnd), rnd.nextInt(101));
                Row address = RowFactory.create(randomString(rnd), rnd.nextInt(9999) + 1);
                Row tuple = RowFactory.create(person, address);
                rows.add(RowFactory.create(rowId++, tuple));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateUdtMapKeyDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", DataTypes.createMapType(personType, DataTypes.StringType, false), true)
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
                int mapSize = rnd.nextInt(6);
                Map<Row, String> personMap = new HashMap<>();
                for (int j = 0; j < mapSize; j++)
                {
                    Row personKey = RowFactory.create(randomString(rnd), rnd.nextInt(101));
                    String value = randomString(rnd);
                    personMap.put(personKey, value);
                }
                rows.add(RowFactory.create(rowId++, personMap));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateUdtWithListTupleDataFrame(SparkSession spark, long seed)
    {
        StructType tupleType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("_1", DataTypes.IntegerType, false),
        DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType udtListTupleType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("items", DataTypes.createArrayType(tupleType, false), true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", udtListTupleType, true)
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
                List<Row> tuples = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int listSize = rnd.nextInt(6);
                    tuples = new ArrayList<>();
                    for (int j = 0; j < listSize; j++)
                    {
                        tuples.add(RowFactory.create(rnd.nextInt(101), randomString(rnd)));
                    }
                }
                Row udt = RowFactory.create(tuples);
                rows.add(RowFactory.create(rowId++, udt));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateUdtWithMapTupleDataFrame(SparkSession spark, long seed)
    {
        StructType tupleType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("_1", DataTypes.IntegerType, false),
        DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType udtMapTupleType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("items", DataTypes.createMapType(DataTypes.StringType, tupleType, false), true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", udtMapTupleType, true)
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
                Map<String, Row> tupleMap = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int mapSize = rnd.nextInt(6);
                    tupleMap = new HashMap<>();
                    for (int j = 0; j < mapSize; j++)
                    {
                        String key = randomString(rnd);
                        Row value = RowFactory.create(rnd.nextInt(101), randomString(rnd));
                        tupleMap.put(key, value);
                    }
                }
                Row udt = RowFactory.create(tupleMap);
                rows.add(RowFactory.create(rowId++, udt));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateTupleWithListUdtDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType tupleType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("_1", DataTypes.IntegerType, false),
        DataTypes.createStructField("_2", DataTypes.createArrayType(personType, false), true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
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
                int tupleInt = rnd.nextInt(101);
                List<Row> persons = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int listSize = rnd.nextInt(6);
                    persons = new ArrayList<>();
                    for (int j = 0; j < listSize; j++)
                    {
                        persons.add(RowFactory.create(randomString(rnd), rnd.nextInt(101)));
                    }
                }
                Row tuple = RowFactory.create(tupleInt, persons);
                rows.add(RowFactory.create(rowId++, tuple));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateTupleWithMapUdtDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType tupleType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("_1", DataTypes.StringType, false),
        DataTypes.createStructField("_2", DataTypes.createMapType(DataTypes.StringType, personType, false), true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
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
                String tupleStr = randomString(rnd);
                Map<String, Row> personMap = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int mapSize = rnd.nextInt(6);
                    personMap = new HashMap<>();
                    for (int j = 0; j < mapSize; j++)
                    {
                        String key = randomString(rnd);
                        Row person = RowFactory.create(randomString(rnd), rnd.nextInt(101));
                        personMap.put(key, person);
                    }
                }
                Row tuple = RowFactory.create(tupleStr, personMap);
                rows.add(RowFactory.create(rowId++, tuple));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateTupleWithSetUdtDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType tupleType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("_1", DataTypes.IntegerType, false),
        DataTypes.createStructField("_2", DataTypes.createArrayType(personType, false), true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
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
                int tupleInt = rnd.nextInt(1001);
                List<Row> personsList = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int setSize = rnd.nextInt(6);
                    personsList = new ArrayList<>();
                    for (int j = 0; j < setSize; j++)
                    {
                        personsList.add(RowFactory.create(randomString(rnd), rnd.nextInt(101)));
                    }
                }
                Row tuple = RowFactory.create(tupleInt, personsList);
                rows.add(RowFactory.create(rowId++, tuple));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateUdtWithListUdtDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType udtContainerType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("inner", DataTypes.createArrayType(personType, false), true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", udtContainerType, true)
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
                List<Row> inner = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int listSize = rnd.nextInt(6);
                    inner = new ArrayList<>();
                    for (int j = 0; j < listSize; j++)
                    {
                        inner.add(RowFactory.create(randomString(rnd), rnd.nextInt(101)));
                    }
                }
                Row udt = RowFactory.create(inner);
                rows.add(RowFactory.create(rowId++, udt));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateUdtWithSetUdtDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType udtSetContainerType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("inner", DataTypes.createArrayType(personType, false), true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", udtSetContainerType, true)
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
                List<Row> persons = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int setSize = rnd.nextInt(6);
                    persons = new ArrayList<>();
                    for (int j = 0; j < setSize; j++)
                    {
                        persons.add(RowFactory.create(randomString(rnd), rnd.nextInt(101)));
                    }
                }
                Row udt = RowFactory.create(persons);
                rows.add(RowFactory.create(rowId++, udt));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateUdtWithMapUdtDataFrame(SparkSession spark, long seed)
    {
        StructType personType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("name", DataTypes.StringType, true),
        DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));

        StructType addressType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("street", DataTypes.StringType, true),
        DataTypes.createStructField("number", DataTypes.IntegerType, true)
        ));

        StructType udtMapContainerType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("inner", DataTypes.createMapType(personType, addressType, false), true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", udtMapContainerType, true)
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
                Map<Row, Row> udtMap = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int mapSize = rnd.nextInt(6);
                    udtMap = new HashMap<>();
                    for (int j = 0; j < mapSize; j++)
                    {
                        Row personKey = RowFactory.create(randomString(rnd), rnd.nextInt(101));
                        Row addressValue = RowFactory.create(randomString(rnd), rnd.nextInt(9999) + 1);
                        udtMap.put(personKey, addressValue);
                    }
                }
                Row udt = RowFactory.create(udtMap);
                rows.add(RowFactory.create(rowId++, udt));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> generateUdtWithSetTupleDataFrame(SparkSession spark, long seed)
    {
        StructType tupleType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("_1", DataTypes.IntegerType, false),
        DataTypes.createStructField("_2", DataTypes.StringType, false)
        ));

        StructType udtSetTupleType = DataTypes.createStructType(List.of(
        DataTypes.createStructField("items", DataTypes.createArrayType(tupleType, false), true)
        ));

        StructType schema = DataTypes.createStructType(List.of(
        DataTypes.createStructField("id", DataTypes.LongType, false),
        DataTypes.createStructField("data", udtSetTupleType, true)
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
                List<Row> tuples = null;
                if (rnd.nextInt(100) >= 20)
                {
                    int setSize = rnd.nextInt(6);
                    tuples = new ArrayList<>();
                    for (int j = 0; j < setSize; j++)
                    {
                        tuples.add(RowFactory.create(rnd.nextInt(101), randomString(rnd)));
                    }
                }
                Row udt = RowFactory.create(tuples);
                rows.add(RowFactory.create(rowId++, udt));
            }
        }

        for (int i = 0; i < MIN_NULL_ROWS; i++)
        {
            rows.add(RowFactory.create(rowId++, null));
        }

        return spark.createDataFrame(rows, schema);
    }

    // ==================== Assertion Context Formatting Methods ====================

    private String formatContext(int rowIndex, String sourceFormatted, String readFormatted)
    {
        return String.format("Row %d mismatch\nSource: %s\nRead:   %s",
                             rowIndex, sourceFormatted, readFormatted);
    }

    private String formatSimpleUdtRow(Row row)
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
        Row udt = row.getStruct(1);
        return String.format("Row(id=%d, data={name='%s', age=%d})",
                             id, udt.getString(0), udt.getInt(1));
    }

    private String formatListUdtRow(Row row)
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
        List<Row> udts = row.getList(1);
        String listStr = udts.stream().limit(3)
                             .map(this::formatPersonUdt)
                             .collect(Collectors.joining(", ", "[", udts.size() > 3 ? String.format("... (%d items)]", udts.size()) : "]"));
        return String.format("Row(id=%d, data=%s)", id, listStr);
    }

    private String formatPersonUdt(Row udt)
    {
        if (udt == null)
        {
            return "NULL";
        }
        return String.format("{name='%s', age=%d}", udt.getString(0), udt.getInt(1));
    }

    private String formatDeeplyNestedUdtRow(Row row)
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
        Row level3 = row.getStruct(1);
        if (level3.isNullAt(0))
        {
            return String.format("Row(id=%d, data={level3: {level2: NULL, field3=%d}})",
                                 id, level3.getLong(1));
        }
        Row level2 = level3.getStruct(0);
        if (level2.isNullAt(0))
        {
            return String.format("Row(id=%d, data={level3: {level2: {level1: NULL, field2=%d}, field3=%d}})",
                                 id, level2.getInt(1), level3.getLong(1));
        }
        Row level1 = level2.getStruct(0);
        return String.format("Row(id=%d, data={level3: {level2: {level1: {field='%s'}, field2=%d}, field3=%d}})",
                             id, level1.getString(0), level2.getInt(1), level3.getLong(1));
    }

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
