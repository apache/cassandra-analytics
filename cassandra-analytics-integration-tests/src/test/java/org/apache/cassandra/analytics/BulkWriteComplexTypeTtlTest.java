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

package org.apache.cassandra.analytics;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.TTLOption;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createMapType;
import static org.apache.spark.sql.types.DataTypes.createStructType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that bulk writes with TTL work correctly for complex types (list, set, map, UDT).
 * This ensures the expiring cell path is invoked correctly for collection and composite types.
 * TODO: add tuple ttl test after adding support for writing tuples
 */
class BulkWriteComplexTypeTtlTest extends SharedClusterSparkIntegrationTestBase
{
    static final int ROW_COUNT = 100;
    static final int TTL_SECONDS = 5;

    static final QualifiedName LIST_TTL_TABLE = new QualifiedName(TEST_KEYSPACE, "test_list_ttl");
    static final QualifiedName SET_TTL_TABLE = new QualifiedName(TEST_KEYSPACE, "test_set_ttl");
    static final QualifiedName MAP_TTL_TABLE = new QualifiedName(TEST_KEYSPACE, "test_map_ttl");
    static final QualifiedName UDT_TTL_TABLE = new QualifiedName(TEST_KEYSPACE, "test_udt_ttl");

    static final String SIMPLE_UDT_NAME = "simple_udt";

    @Test
    void testComplexTypesWithTtl()
    {
        SparkSession spark = getOrCreateSparkSession();

        // Write all complex types with TTL
        writeListData(spark);
        writeSetData(spark);
        writeMapData(spark);
        writeUdtData(spark);

        // Wait for TTL to expire (TTL + 1 second margin)
        Uninterruptibles.sleepUninterruptibly(TTL_SECONDS + 1, TimeUnit.SECONDS);

        // Verify all types have expired
        assertThat(bulkReaderDataFrame(LIST_TTL_TABLE).load().collectAsList())
            .as("list TTL post-expiry")
            .isEmpty();
        assertThat(bulkReaderDataFrame(SET_TTL_TABLE).load().collectAsList())
            .as("set TTL post-expiry")
            .isEmpty();
        assertThat(bulkReaderDataFrame(MAP_TTL_TABLE).load().collectAsList())
            .as("map TTL post-expiry")
            .isEmpty();
        assertThat(bulkReaderDataFrame(UDT_TTL_TABLE).load().collectAsList())
            .as("UDT TTL post-expiry")
            .isEmpty();
    }

    private void writeListData(SparkSession spark)
    {
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("listdata", createArrayType(IntegerType), false);

        List<Row> rows = IntStream.range(0, ROW_COUNT)
                                  .mapToObj(i -> RowFactory.create(i, Arrays.asList(i, i + 1)))
                                  .collect(Collectors.toList());
        Dataset<Row> df = spark.createDataFrame(rows, schema);

        bulkWriterDataFrameWriter(df, LIST_TTL_TABLE).option(WriterOptions.TTL.name(), TTLOption.constant(TTL_SECONDS))
                                                     .save();
    }

    private void writeSetData(SparkSession spark)
    {
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("setdata", createArrayType(StringType), false);

        List<Row> rows = IntStream.range(0, ROW_COUNT)
                                  .mapToObj(i -> RowFactory.create(i, ImmutableSet.of("item" + i)))
                                  .collect(Collectors.toList());
        Dataset<Row> df = spark.createDataFrame(rows, schema);

        bulkWriterDataFrameWriter(df, SET_TTL_TABLE).option(WriterOptions.TTL.name(), TTLOption.constant(TTL_SECONDS))
                                                    .save();
    }

    private void writeMapData(SparkSession spark)
    {
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("mapdata", createMapType(StringType, IntegerType), false);

        List<Row> rows = IntStream.range(0, ROW_COUNT)
                                  .mapToObj(i -> RowFactory.create(i, ImmutableMap.of("key" + i, i)))
                                  .collect(Collectors.toList());
        Dataset<Row> df = spark.createDataFrame(rows, schema);

        bulkWriterDataFrameWriter(df, MAP_TTL_TABLE).option(WriterOptions.TTL.name(), TTLOption.constant(TTL_SECONDS))
                                                    .save();
    }

    private void writeUdtData(SparkSession spark)
    {
        StructType udtType = createStructType(new StructField[]{
            new StructField("f1", StringType, true, Metadata.empty()),
            new StructField("f2", IntegerType, true, Metadata.empty())
        });
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("udtfield", udtType, false);

        List<Row> rows = IntStream.range(0, ROW_COUNT)
                                  .mapToObj(i -> RowFactory.create(i, RowFactory.create("course" + i, i)))
                                  .collect(Collectors.toList());
        Dataset<Row> df = spark.createDataFrame(rows, schema);

        bulkWriterDataFrameWriter(df, UDT_TTL_TABLE).option(WriterOptions.TTL.name(), TTLOption.constant(TTL_SECONDS))
                                                    .save();
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(LIST_TTL_TABLE, DC1_RF3);

        cluster.schemaChangeIgnoringStoppedInstances("CREATE TYPE " + TEST_KEYSPACE + "." + SIMPLE_UDT_NAME
                                                     + " (f1 text, f2 int)");

        cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE " + LIST_TTL_TABLE + " ("
                                                     + "id int PRIMARY KEY, "
                                                     + "listdata list<int>)");

        cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE " + SET_TTL_TABLE + " ("
                                                     + "id int PRIMARY KEY, "
                                                     + "setdata set<text>)");

        cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE " + MAP_TTL_TABLE + " ("
                                                     + "id int PRIMARY KEY, "
                                                     + "mapdata map<text, int>)");

        cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE " + UDT_TTL_TABLE + " ("
                                                     + "id int PRIMARY KEY, "
                                                     + "udtfield " + SIMPLE_UDT_NAME + ")");
    }
}
