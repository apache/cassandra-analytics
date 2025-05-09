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

import java.util.function.Predicate;
import com.datastax.driver.core.UDTValue;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

class BulkWriteUdtTest extends SharedClusterSparkIntegrationTestBase
{
    static final QualifiedName UDT_TABLE_NAME = new QualifiedName(TEST_KEYSPACE, "test_udt");
    static final QualifiedName NESTED_TABLE_NAME = new QualifiedName(TEST_KEYSPACE, "test_nested_udt");
    public static final String TWO_FIELD_UDT_NAME = "two_field_udt";
    public static final String NESTED_FIELD_UDT_NAME = "nested_udt";
    public static final String UDT_TABLE_CREATE = "CREATE TABLE " + UDT_TABLE_NAME + " (\n"
                                                  + "          id BIGINT PRIMARY KEY,\n"
                                                  + "          udtfield " + TWO_FIELD_UDT_NAME + ");";
    public static final String TWO_FIELD_UDT_DEF = "CREATE TYPE " + UDT_TABLE_NAME.keyspace() + "."
                                                   + TWO_FIELD_UDT_NAME + " (\n"
                                                   + "            f1 text,\n"
                                                   + "            f2 int);";
    public static final String NESTED_UDT_DEF = "CREATE TYPE " + NESTED_TABLE_NAME.keyspace() + "."
                                                + NESTED_FIELD_UDT_NAME + " (\n"
                                                + "            n1 BIGINT,\n"
                                                + "            n2 frozen<" + TWO_FIELD_UDT_NAME + ">"
                                                + ");";
    public static final String NESTED_TABLE_CREATE = "CREATE TABLE " + NESTED_TABLE_NAME + "(\n"
                                                     + "           id BIGINT PRIMARY KEY,\n"
                                                     + "           nested " + NESTED_FIELD_UDT_NAME + ");";

    // UDT with list, set and map in it
    public static final String UDT_WITH_COLLECTIONS_TYPE_NAME = "udt_with_collections";
    public static final String UDT_WITH_COLLECTIONS_TYPE_CREATE = "CREATE TYPE " + TEST_KEYSPACE + "." + UDT_WITH_COLLECTIONS_TYPE_NAME +
            " (f1 list<text>, f2 set<int>, f3 map<int, text>);";

    // table with list of UDTs, and UDT itself has collections in it
    public static final QualifiedName LIST_OF_UDT_SOURCE_TABLE = new QualifiedName(TEST_KEYSPACE, "list_of_udt_src");
    public static final QualifiedName LIST_OF_UDT_DEST_TABLE = new QualifiedName(TEST_KEYSPACE, "list_of_udt_dest");
    public static final String LIST_OF_UDT_TABLE_CREATE = "CREATE TABLE %s.%s (\n"
            + "            id BIGINT PRIMARY KEY,\n"
            + "            udtlist frozen<list<frozen<" + UDT_WITH_COLLECTIONS_TYPE_NAME + ">>>)";

    // table with set of UDTs, and UDT itself has collections in it
    public static final QualifiedName SET_OF_UDT_SOURCE_TABLE = new QualifiedName(TEST_KEYSPACE, "set_of_udt_src");
    public static final QualifiedName SET_OF_UDT_DEST_TABLE = new QualifiedName(TEST_KEYSPACE, "set_of_udt_dest");
    public static final String SET_OF_UDT_TABLE_CREATE = "CREATE TABLE %s.%s (\n"
            + "            id BIGINT PRIMARY KEY,\n"
            + "            udtset frozen<set<frozen<" + UDT_WITH_COLLECTIONS_TYPE_NAME + ">>>)";

    // table with map of UDTs, and UDT itself has collections in it
    public static final QualifiedName MAP_OF_UDT_SOURCE_TABLE = new QualifiedName(TEST_KEYSPACE, "map_of_udt_src");
    public static final QualifiedName MAP_OF_UDT_DEST_TABLE = new QualifiedName(TEST_KEYSPACE, "map_of_udt_dest");
    public static final String MAP_OF_UDT_TABLE_CREATE = "CREATE TABLE %s.%s (\n"
            + "            id BIGINT PRIMARY KEY,\n"
            + "            udtmap frozen<map<frozen<" + UDT_WITH_COLLECTIONS_TYPE_NAME + ">, frozen<" + UDT_WITH_COLLECTIONS_TYPE_NAME + ">>>)";

    // udt with list of UDTs inside it
    public static final String UDT_WITH_LIST_OF_UDT_TYPE_NAME = "udt_with_list_of_udt_type";
    public static final String UDT_WITH_LIST_OF_UDT_TYPE_CREATE = "CREATE TYPE " + TEST_KEYSPACE + "." + UDT_WITH_LIST_OF_UDT_TYPE_NAME +
            " (innerudt list<frozen<" + TWO_FIELD_UDT_NAME + ">>);";
    public static final QualifiedName UDT_WITH_LIST_OF_UDT_SOURCE_TABLE = new QualifiedName(TEST_KEYSPACE, "udt_with_list_of_udt_src");
    public static final QualifiedName UDT_WITH_LIST_OF_UDT_DEST_TABLE = new QualifiedName(TEST_KEYSPACE, "udt_with_list_of_udt_dest");

    // udt with set of UDTs inside it
    public static final String UDT_WITH_SET_OF_UDT_TYPE_NAME = "udt_with_set_of_udt_type";
    public static final String UDT_WITH_SET_OF_UDT_TYPE_CREATE = "CREATE TYPE " + TEST_KEYSPACE + "." + UDT_WITH_SET_OF_UDT_TYPE_NAME +
            " (innerudt set<frozen<" + TWO_FIELD_UDT_NAME + ">>);";
    public static final QualifiedName UDT_WITH_SET_OF_UDT_SOURCE_TABLE = new QualifiedName(TEST_KEYSPACE, "udt_with_set_of_udt_src");
    public static final QualifiedName UDT_WITH_SET_OF_UDT_DEST_TABLE = new QualifiedName(TEST_KEYSPACE, "udt_with_set_of_udt_dest");

    // udt with map of UDTs inside it
    public static final String UDT_WITH_MAP_OF_UDT_TYPE_NAME = "udt_with_map_of_udt_type";
    public static final String UDT_WITH_MAP_OF_UDT_TYPE_CREATE = "CREATE TYPE " + TEST_KEYSPACE + "." + UDT_WITH_MAP_OF_UDT_TYPE_NAME +
            " (innerudt map<frozen<" + TWO_FIELD_UDT_NAME + ">, frozen<" + TWO_FIELD_UDT_NAME + ">>);";
    public static final QualifiedName UDT_WITH_MAP_OF_UDT_SOURCE_TABLE = new QualifiedName(TEST_KEYSPACE, "udt_with_map_of_udt_src");
    public static final QualifiedName UDT_WITH_MAP_OF_UDT_DEST_TABLE = new QualifiedName(TEST_KEYSPACE, "udt_with_map_of_udt_dest");

    // Table with UDT which contains either a list or set or map of UDTs inside it
    public static final String UDT_WITH_COLLECTION_OF_UDT_TABLE_CREATE = "CREATE TABLE %s.%s (\n"
            + "            id BIGINT PRIMARY KEY,\n"
            + "            outerudt frozen<%s>)";

    private ICoordinator coordinator;


    @Test
    void testWriteWithUdt()
    {
        SparkSession spark = getOrCreateSparkSession();
        Predicate<Integer> nullSetter = index -> index % 2 == 0;
        Dataset<Row> df = DataGenerationUtils.generateUdtData(spark, ROW_COUNT, nullSetter);

        bulkWriterDataFrameWriter(df, UDT_TABLE_NAME).save();

        SimpleQueryResult result = coordinator.executeWithResult("SELECT * FROM " + UDT_TABLE_NAME, ConsistencyLevel.ALL);
        assertThat(result.hasNext()).isTrue();
        validateWritesWithDriverResultSet(df.collectAsList(),
                                          queryAllDataWithDriver(UDT_TABLE_NAME),
                                          BulkWriteUdtTest::udtRowFormatter);
    }

    @Test
    void testWriteWithNestedUdt()
    {
        SparkSession spark = getOrCreateSparkSession();
        Predicate<Integer> nullSetter = index -> index % 2 == 0;
        Dataset<Row> df = DataGenerationUtils.generateNestedUdtData(spark, ROW_COUNT, nullSetter);

        bulkWriterDataFrameWriter(df, NESTED_TABLE_NAME).save();

        SimpleQueryResult result = coordinator.executeWithResult("SELECT * FROM " + NESTED_TABLE_NAME, ConsistencyLevel.ALL);
        assertThat(result.hasNext()).isTrue();
        validateWritesWithDriverResultSet(df.collectAsList(),
                                          queryAllDataWithDriver(NESTED_TABLE_NAME),
                                          BulkWriteUdtTest::udtRowFormatter);
    }

    @Test
    void testListOfUdts()
    {
        int numRowsInserted = populateListOfUdts();

        // Create a spark frame with the data inserted during the setup
        Dataset<Row> sourceData = bulkReaderDataFrame(LIST_OF_UDT_SOURCE_TABLE).load();
        assertThat(sourceData.count()).isEqualTo(numRowsInserted);

        // Insert the dataset containing list of UDTs, and UDT itself has collections in it
        bulkWriterDataFrameWriter(sourceData, LIST_OF_UDT_DEST_TABLE).save();
        validateWritesWithDriverResultSet(sourceData.collectAsList(),
                queryAllDataWithDriver(LIST_OF_UDT_DEST_TABLE),
                BulkWriteUdtTest::listOfUdtRowFormatter);
    }

    private int populateListOfUdts()
    {
        // table(id, list<udt(list<>, set<>, map<>)>)
        // insert list of UDTs, and each UDT has a list, set and map
        String insertIntoListOfUdts = "INSERT INTO %s (id, udtlist) VALUES (%d, [{f1:['value %d'], f2:{%d}, f3:{%d : 'value %d'}}])";

        int i = 0;
        for (; i < ROW_COUNT; i++)
        {
            coordinator.execute(String.format(insertIntoListOfUdts, LIST_OF_UDT_SOURCE_TABLE, i, i, i, i, i), ConsistencyLevel.ALL);
        }

        // test null cases
        coordinator.execute(String.format("insert into %s (id) values (%d)",
                                          LIST_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, udtlist) values (%d, null)",
                                          LIST_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, udtlist) values (%d, [{f1:null, f2:null, f3:null}])",
                                          LIST_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);

        return i;
    }

    @Test
    void testSetOfUdts()
    {
        int numRowsInserted = populateSetOfUdts();
        // Create a spark frame with the data inserted during the setup
        Dataset<Row> sourceData = bulkReaderDataFrame(SET_OF_UDT_SOURCE_TABLE).load();
        assertThat(sourceData.count()).isEqualTo(numRowsInserted);

        // Insert the dataset containing set of UDTs, and UDT itself has collections in it
        bulkWriterDataFrameWriter(sourceData, SET_OF_UDT_DEST_TABLE).save();
        validateWritesWithDriverResultSet(sourceData.collectAsList(),
                queryAllDataWithDriver(SET_OF_UDT_DEST_TABLE),
                BulkWriteUdtTest::setOfUdtRowFormatter);
    }

    private int populateSetOfUdts()
    {
        // table(id, set<udt(list<>, set<>, map<>)>)
        // insert set of UDTs, and UDT has a list, set and map inside it
        String insertIntoSetOfUdts = "INSERT INTO %s (id, udtset) VALUES (%d, " +
                "{{f1:['value %d'], f2:{%d}, f3:{%d : 'value %d'}}})";

        int i = 0;
        for (; i < ROW_COUNT; i++)
        {
            cluster.schemaChangeIgnoringStoppedInstances(String.format(insertIntoSetOfUdts, SET_OF_UDT_SOURCE_TABLE,
                    i, i, i, i, i));
        }

        // test null cases
        coordinator.execute(String.format("insert into %s (id) values (%d)",
                                          SET_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, udtset) values (%d, null)",
                                          SET_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, udtset) values (%d, {{f1:null, f2:null, f3:null}})",
                                          SET_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);

        return i;
    }

    @Test
    void testMapOfUdts()
    {
        int numRowsInserted = populateMapOfUdts();
        // Create a spark frame with the data inserted during the setup
        Dataset<Row> sourceData = bulkReaderDataFrame(MAP_OF_UDT_SOURCE_TABLE).load();
        assertThat(sourceData.count()).isEqualTo(numRowsInserted);

        // Insert the dataset containing map of UDTs, and UDT itself has collections in it
        bulkWriterDataFrameWriter(sourceData, MAP_OF_UDT_DEST_TABLE).save();
        validateWritesWithDriverResultSet(sourceData.collectAsList(),
                queryAllDataWithDriver(MAP_OF_UDT_DEST_TABLE),
                BulkWriteUdtTest::mapOfUdtRowFormatter);
    }

    private int populateMapOfUdts()
    {
        // table(id, map<udt(list<>, set<>, map<>), udt(list<>, set<>, map<>)>)
        // insert map of UDTs, and UDT has a list, set and map inside it
        String insertIntoMapOfUdts = "INSERT INTO %s (id, udtmap) VALUES (%d, " +
                "{{f1:['value %d'], f2:{%d}, f3:{%d : 'value %d'}} : {f1:['value %d'], f2:{%d}, f3:{%d : 'value %d'}}})";

        int i = 0;
        for (; i < ROW_COUNT; i++)
        {
            cluster.schemaChangeIgnoringStoppedInstances(String.format(insertIntoMapOfUdts, MAP_OF_UDT_SOURCE_TABLE,
                    i, i, i, i, i, i, i, i, i));
        }

        coordinator.execute(String.format("insert into %s (id) values (%d)",
                                          MAP_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, udtmap) values (%d, null)",
                                          MAP_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, udtmap) values (%d, {{f1:null, f2:null, f3:null} : {f1:null, f2:null, f3:null}})",
                                          MAP_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);

        return i;
    }

    @Test
    void testUdtWithListOfUdts()
    {
        int numRowsInserted = populateUdtWithListOfUdts();

        // Create a spark frame with the data inserted during the setup
        Dataset<Row> sourceData = bulkReaderDataFrame(UDT_WITH_LIST_OF_UDT_SOURCE_TABLE).load();
        assertThat(sourceData.count()).isEqualTo(numRowsInserted);

        // Insert the dataset containing list of UDTs, and UDT itself has collections in it
        bulkWriterDataFrameWriter(sourceData, UDT_WITH_LIST_OF_UDT_DEST_TABLE).save();
        validateWritesWithDriverResultSet(sourceData.collectAsList(),
                queryAllDataWithDriver(UDT_WITH_LIST_OF_UDT_DEST_TABLE),
                BulkWriteUdtTest::udtRowFormatter);
    }

    private int populateUdtWithListOfUdts()
    {
        // table(id, udt<list<udt(f1 text, f2 int)>>)
        String insertIntoUdtWithListOfUdts = "INSERT INTO %s (id, outerudt) VALUES (%d, {innerudt:[{f1:'value %d', f2:%d}]})";

        int i = 0;
        for (; i < ROW_COUNT; i++)
        {
            cluster.schemaChangeIgnoringStoppedInstances(String.format(insertIntoUdtWithListOfUdts, UDT_WITH_LIST_OF_UDT_SOURCE_TABLE, i, i, i, i, i));
        }

        // test null cases
        coordinator.execute(String.format("insert into %s (id) values (%d)",
                                          UDT_WITH_LIST_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, outerudt) values (%d, null)",
                                          UDT_WITH_LIST_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, outerudt) values (%d, {innerudt:[]})",
                                          UDT_WITH_LIST_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, outerudt) values (%d, {innerudt:[{f1:null, f2:null}]})",
                                          UDT_WITH_LIST_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);

        return i;
    }

    @Test
    void testUdtWithSetOfUdts()
    {
        int numRowsInserted = populateUdtWithSetOfUdts();

        // Create a spark frame with the data inserted during the setup
        Dataset<Row> sourceData = bulkReaderDataFrame(UDT_WITH_SET_OF_UDT_SOURCE_TABLE).load();
        assertThat(sourceData.count()).isEqualTo(numRowsInserted);

        // Insert the dataset containing list of UDTs, and UDT itself has collections in it
        bulkWriterDataFrameWriter(sourceData, UDT_WITH_SET_OF_UDT_DEST_TABLE).save();
        validateWritesWithDriverResultSet(sourceData.collectAsList(),
                queryAllDataWithDriver(UDT_WITH_SET_OF_UDT_DEST_TABLE),
                BulkWriteUdtTest::udtRowFormatter);
    }

    private int populateUdtWithSetOfUdts()
    {
        // table(id, udt<set<udt(f1 text, f2 int)>>)
        String insertIntoUdtWithSetOfUdts = "INSERT INTO %s (id, outerudt) VALUES (%d, {innerudt:{{f1:'value %d', f2:%d}}})";

        int i = 0;
        for (; i < ROW_COUNT; i++)
        {
            cluster.schemaChangeIgnoringStoppedInstances(String.format(insertIntoUdtWithSetOfUdts, UDT_WITH_SET_OF_UDT_SOURCE_TABLE, i, i, i, i, i));
        }

        // test null cases
        coordinator.execute(String.format("insert into %s (id) values (%d)",
                                          UDT_WITH_SET_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, outerudt) values (%d, null)",
                                          UDT_WITH_SET_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, outerudt) values (%d, {innerudt:{}})",
                                          UDT_WITH_SET_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, outerudt) values (%d, {innerudt:{{f1:null, f2:null}}})",
                                          UDT_WITH_SET_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);

        return i;
    }

    @Test
    void testUdtWithMapOfUdts()
    {
        int numRowsInserted = populateUdtWithMapOfUdts();

        // Create a spark frame with the data inserted during the setup
        Dataset<Row> sourceData = bulkReaderDataFrame(UDT_WITH_MAP_OF_UDT_SOURCE_TABLE).load();
        assertThat(sourceData.count()).isEqualTo(numRowsInserted);

        // Insert the dataset containing list of UDTs, and UDT itself has collections in it
        bulkWriterDataFrameWriter(sourceData, UDT_WITH_MAP_OF_UDT_DEST_TABLE).save();
        validateWritesWithDriverResultSet(sourceData.collectAsList(),
                queryAllDataWithDriver(UDT_WITH_MAP_OF_UDT_DEST_TABLE),
                BulkWriteUdtTest::udtRowFormatter);
    }

    private int populateUdtWithMapOfUdts()
    {
        // table(id, udt<map<udt(f1 text, f2 int), udt(f1 text, f2 int)>>)
        String insertIntoUdtWithMapOfUdts = "INSERT INTO %s (id, outerudt) VALUES (%d, {innerudt:{{f1:'valueA %d', f2:%d}: {f1:'valueB %d', f2:%d}}})";

        int i = 0;
        for (; i < ROW_COUNT; i++)
        {
            cluster.schemaChangeIgnoringStoppedInstances(String.format(insertIntoUdtWithMapOfUdts, UDT_WITH_MAP_OF_UDT_SOURCE_TABLE, i, i, i, i, i));
        }

        // test null cases
        coordinator.execute(String.format("insert into %s (id) values (%d)",
                                          UDT_WITH_MAP_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, outerudt) values (%d, null)",
                                          UDT_WITH_MAP_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.execute(String.format("insert into %s (id, outerudt) values (%d, {innerudt:{{f1:null, f2:null}: {f1:null, f2:null}}})",
                                          UDT_WITH_MAP_OF_UDT_SOURCE_TABLE, i++), ConsistencyLevel.ALL);

        return i;
    }

    @NotNull
    public static String udtRowFormatter(com.datastax.driver.core.Row row)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(row.getLong(0)).append(":");
        UDTValue udt = row.getUDTValue(1);
        if (udt == null)
        {
            sb.append("null");
        }
        else
        {
            sb.append(udt);
        }

        return sb.toString();
    }

    @NotNull
    public static String listOfUdtRowFormatter(com.datastax.driver.core.Row row)
    {
        return row.getLong(0) +
                ":" +
                row.getList(1, UDTValue.class).toString()
                        .replace("{}", "null")
                        .replace("[]", "null");
    }

    @NotNull
    public static String setOfUdtRowFormatter(com.datastax.driver.core.Row row)
    {
        // Formats as field:value with no whitespaces, and strings quoted
        // Driver Codec writes "NULL" for null value. Spark DF writes "null".
        return row.getLong(0) +
                ":" +
                row.getSet(1, UDTValue.class).toString()
                        .replace("{}", "null")
                        .replace("[]", "null");
    }

    @NotNull
    public static String mapOfUdtRowFormatter(com.datastax.driver.core.Row row)
    {
        // Formats as field:value with no whitespaces, and strings quoted
        // Driver Codec writes "NULL" for null value. Spark DF writes "null".
        return row.getLong(0) +
                ":" +
                row.getMap(1, UDTValue.class, UDTValue.class).toString()
                        .replace("=", ":")
                        .replace("{}", "null")
                        .replace("[]", "null");
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
        coordinator = cluster.getFirstRunningInstance().coordinator();

        createTestKeyspace(UDT_TABLE_NAME, DC1_RF3);

        cluster.schemaChangeIgnoringStoppedInstances(TWO_FIELD_UDT_DEF);
        cluster.schemaChangeIgnoringStoppedInstances(NESTED_UDT_DEF);
        cluster.schemaChangeIgnoringStoppedInstances(UDT_TABLE_CREATE);
        cluster.schemaChangeIgnoringStoppedInstances(NESTED_TABLE_CREATE);
        cluster.schemaChangeIgnoringStoppedInstances(UDT_WITH_COLLECTIONS_TYPE_CREATE);
        cluster.schemaChangeIgnoringStoppedInstances(UDT_WITH_LIST_OF_UDT_TYPE_CREATE);
        cluster.schemaChangeIgnoringStoppedInstances(UDT_WITH_SET_OF_UDT_TYPE_CREATE);
        cluster.schemaChangeIgnoringStoppedInstances(UDT_WITH_MAP_OF_UDT_TYPE_CREATE);

        cluster.schemaChangeIgnoringStoppedInstances(String.format(LIST_OF_UDT_TABLE_CREATE,
                LIST_OF_UDT_SOURCE_TABLE.keyspace(),
                LIST_OF_UDT_SOURCE_TABLE.table()));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(LIST_OF_UDT_TABLE_CREATE,
                LIST_OF_UDT_DEST_TABLE.keyspace(),
                LIST_OF_UDT_DEST_TABLE.table()));

        cluster.schemaChangeIgnoringStoppedInstances(String.format(SET_OF_UDT_TABLE_CREATE,
                SET_OF_UDT_SOURCE_TABLE.keyspace(),
                SET_OF_UDT_SOURCE_TABLE.table()));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(SET_OF_UDT_TABLE_CREATE,
                SET_OF_UDT_DEST_TABLE.keyspace(),
                SET_OF_UDT_DEST_TABLE.table()));

        cluster.schemaChangeIgnoringStoppedInstances(String.format(MAP_OF_UDT_TABLE_CREATE,
                MAP_OF_UDT_SOURCE_TABLE.keyspace(),
                MAP_OF_UDT_SOURCE_TABLE.table()));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(MAP_OF_UDT_TABLE_CREATE,
                MAP_OF_UDT_DEST_TABLE.keyspace(),
                MAP_OF_UDT_DEST_TABLE.table()));

        cluster.schemaChangeIgnoringStoppedInstances(String.format(UDT_WITH_COLLECTION_OF_UDT_TABLE_CREATE,
                UDT_WITH_LIST_OF_UDT_SOURCE_TABLE.keyspace(),
                UDT_WITH_LIST_OF_UDT_SOURCE_TABLE.table(),
                UDT_WITH_LIST_OF_UDT_TYPE_NAME));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(UDT_WITH_COLLECTION_OF_UDT_TABLE_CREATE,
                UDT_WITH_LIST_OF_UDT_DEST_TABLE.keyspace(),
                UDT_WITH_LIST_OF_UDT_DEST_TABLE.table(),
                UDT_WITH_LIST_OF_UDT_TYPE_NAME));

        cluster.schemaChangeIgnoringStoppedInstances(String.format(UDT_WITH_COLLECTION_OF_UDT_TABLE_CREATE,
                UDT_WITH_SET_OF_UDT_SOURCE_TABLE.keyspace(),
                UDT_WITH_SET_OF_UDT_SOURCE_TABLE.table(),
                UDT_WITH_SET_OF_UDT_TYPE_NAME));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(UDT_WITH_COLLECTION_OF_UDT_TABLE_CREATE,
                UDT_WITH_SET_OF_UDT_DEST_TABLE.keyspace(),
                UDT_WITH_SET_OF_UDT_DEST_TABLE.table(),
                UDT_WITH_SET_OF_UDT_TYPE_NAME));

        cluster.schemaChangeIgnoringStoppedInstances(String.format(UDT_WITH_COLLECTION_OF_UDT_TABLE_CREATE,
                UDT_WITH_MAP_OF_UDT_SOURCE_TABLE.keyspace(),
                UDT_WITH_MAP_OF_UDT_SOURCE_TABLE.table(),
                UDT_WITH_MAP_OF_UDT_TYPE_NAME));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(UDT_WITH_COLLECTION_OF_UDT_TABLE_CREATE,
                UDT_WITH_MAP_OF_UDT_DEST_TABLE.keyspace(),
                UDT_WITH_MAP_OF_UDT_DEST_TABLE.table(),
                UDT_WITH_MAP_OF_UDT_TYPE_NAME));
    }
}
