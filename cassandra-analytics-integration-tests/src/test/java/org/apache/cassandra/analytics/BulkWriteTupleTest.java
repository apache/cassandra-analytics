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

import com.datastax.driver.core.TupleValue;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

class BulkWriteTupleTest extends SharedClusterSparkIntegrationTestBase
{
    public static final QualifiedName TUPLE_SOURCE_TABLE = new QualifiedName(TEST_KEYSPACE, "tuple_src");
    public static final QualifiedName TUPLE_DEST_TABLE = new QualifiedName(TEST_KEYSPACE, "tuple_dest");
    public static final String TUPLE_TABLE_CREATE = "CREATE TABLE %s.%s (\n"
                                                    + "            id BIGINT PRIMARY KEY,\n"
                                                    + "            udttuple frozen<tuple<int, text>>)";

    // UDT with collections in it (list, set, map and tuple) in it
    public static final String UDT_WITH_COLLECTIONS_TYPE_NAME = "udt_with_collections";
    public static final String UDT_WITH_COLLECTIONS_TYPE_CREATE = "CREATE TYPE " + TEST_KEYSPACE + "." + UDT_WITH_COLLECTIONS_TYPE_NAME +
            " (f1 list<text>, f2 set<text>, f3 map<int, text>, f4 tuple<int, text>);";

    public static final QualifiedName TUPLE_WITH_UDT_WITH_TUPLE_SOURCE_TABLE = new QualifiedName(TEST_KEYSPACE, "tuple_with_udt_with_tuple_src");
    public static final QualifiedName TUPLE_WITH_UDT_WITH_TUPLE_DEST_TABLE = new QualifiedName(TEST_KEYSPACE, "tuple_with_udt_with_tuple_dest");
    // Table with a tuple, which contains a UDT which in-turn contains collections including nested tuple
    public static final String TUPLE_WITH_UDT_TABLE_CREATE = "CREATE TABLE %s.%s (id BIGINT PRIMARY KEY,\n"
            + "            tuplewithudt frozen<tuple<int, udt_with_collections>>)";

    private ICoordinator coordinator;

    @Test
    void testSimpleTuples()
    {
        int numRowsInserted = populateSimpleTuples();
        Dataset<Row> sourceData = bulkReaderDataFrame(TUPLE_SOURCE_TABLE).load();
        assertThat(sourceData.count()).isEqualTo(numRowsInserted);

        bulkWriterDataFrameWriter(sourceData, TUPLE_DEST_TABLE).save();
        validateWritesWithDriverResultSet(sourceData.collectAsList(),
                queryAllDataWithDriver(TUPLE_DEST_TABLE),
                BulkWriteTupleTest::tupleRowFormatter);
    }

    private int populateSimpleTuples()
    {
        String insertIntoTuples = "INSERT INTO %s (id, udttuple) VALUES (%d, (%d, 'value %d'))";
        int i = 0;
        for (; i < 1; i++)
        {
            coordinator.executeWithResult(String.format(insertIntoTuples, TUPLE_SOURCE_TABLE, i, i, i),
                    ConsistencyLevel.ALL);
        }

        // test null cases
        coordinator.executeWithResult(String.format("insert into %s (id) values (%d)",
                TUPLE_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.executeWithResult(String.format("insert into %s (id, udttuple) values (%d, null)",
                TUPLE_SOURCE_TABLE, i++), ConsistencyLevel.ALL);

        return i;
    }

    @Test
    void testTupleWithUdtWithTuple()
    {
        int numRowsInserted = populateTupleWithUdtWithTuple();

        // Create a spark frame with the data inserted during the setup
        Dataset<Row> sourceData = bulkReaderDataFrame(TUPLE_WITH_UDT_WITH_TUPLE_SOURCE_TABLE).load();
        assertThat(sourceData.count()).isEqualTo(numRowsInserted);

        // Insert the dataset containing list of UDTs, and UDT itself has collections in it
        bulkWriterDataFrameWriter(sourceData, TUPLE_WITH_UDT_WITH_TUPLE_DEST_TABLE).save();
        validateWritesWithDriverResultSet(sourceData.collectAsList(),
                queryAllDataWithDriver(TUPLE_WITH_UDT_WITH_TUPLE_DEST_TABLE),
                BulkWriteTupleTest::tupleRowFormatter);
    }

    private int populateTupleWithUdtWithTuple()
    {
        // table(id, tuple<float, udt_with_collections(list<>, set<>, map<>, tuple<>)>)
        // insert list of UDTs, and each UDT has a list, set and map
        String insertIntoTupleOfUdts = "INSERT INTO %s (id, tuplewithudt) VALUES (%d, " +
                "(%d, {f1:['list value %d'], f2:{'set value %d'}, f3:{%d : 'map value %d'}, f4:(%d, 'tuple value %d')}))";

        int i = 0;
        for (; i < ROW_COUNT; i++)
        {
            coordinator.executeWithResult(String.format(insertIntoTupleOfUdts,
                    TUPLE_WITH_UDT_WITH_TUPLE_SOURCE_TABLE, i, i, i, i, i, i, i, i), ConsistencyLevel.ALL);
        }

        // test null cases
        coordinator.executeWithResult(String.format("insert into %s (id) values (%d)",
                TUPLE_WITH_UDT_WITH_TUPLE_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.executeWithResult(String.format("insert into %s (id, tuplewithudt) values (%d, null)",
                TUPLE_WITH_UDT_WITH_TUPLE_SOURCE_TABLE, i++), ConsistencyLevel.ALL);
        coordinator.executeWithResult(String.format("insert into %s (id, tuplewithudt) values (%d, " +
                        "(null, {f1:null, f2:null, f3:null, f4:null}))",
                TUPLE_WITH_UDT_WITH_TUPLE_SOURCE_TABLE, i++), ConsistencyLevel.ALL);

        return i;
    }

    @NotNull
    public static String tupleRowFormatter(com.datastax.driver.core.Row row)
    {
        String resultRow = row.getLong(0) + ":";
        TupleValue tupleValue = row.getTupleValue(1);
        if (tupleValue == null)
        {
            return resultRow + "null";
        }

        return (resultRow + tupleValue)
               // empty collections have different formatting between driver and spark
               .replace("{}", "null")
               .replace("[]", "null")
               // driver writes lists as [] and sets as {},
               // whereas spark entries have the same type Seq for both lists and sets
               .replace('[', '{')
               .replace(']', '}')
               // Driver writes tuples inside (), whereas
               // Spark considers tuples as type GenericSchemaRow and uses {}
               .replace('(', '{')
               .replace(')', '}');
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

        createTestKeyspace(TUPLE_SOURCE_TABLE, DC1_RF3);
        cluster.schemaChangeIgnoringStoppedInstances(UDT_WITH_COLLECTIONS_TYPE_CREATE);

        cluster.schemaChangeIgnoringStoppedInstances(String.format(TUPLE_TABLE_CREATE, TUPLE_SOURCE_TABLE.keyspace(), TUPLE_SOURCE_TABLE.table()));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(TUPLE_TABLE_CREATE, TUPLE_DEST_TABLE.keyspace(), TUPLE_DEST_TABLE.table()));

        cluster.schemaChangeIgnoringStoppedInstances(String.format(TUPLE_WITH_UDT_TABLE_CREATE,
                TUPLE_WITH_UDT_WITH_TUPLE_SOURCE_TABLE.keyspace(),
                TUPLE_WITH_UDT_WITH_TUPLE_SOURCE_TABLE.table()));
        cluster.schemaChangeIgnoringStoppedInstances(String.format(TUPLE_WITH_UDT_TABLE_CREATE,
                TUPLE_WITH_UDT_WITH_TUPLE_DEST_TABLE.keyspace(),
                TUPLE_WITH_UDT_WITH_TUPLE_DEST_TABLE.table()));
    }
}
