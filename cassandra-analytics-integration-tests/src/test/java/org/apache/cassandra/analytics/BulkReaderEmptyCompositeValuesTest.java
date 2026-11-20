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

import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

public class BulkReaderEmptyCompositeValuesTest extends SharedClusterSparkIntegrationTestBase
{
    private static final String UDT_NAME = "test_empty_fields";

    private static final String EMPTY_FROZEN_UDT = "{text_value: '', blob_value: 0x, null_value: null, tail_value: 'tail'}";
    // TODO: Set null_value to NULL once CASSANALYTICS-217 is fixed.
    private static final String EMPTY_NON_FROZEN_UDT = "{text_value: '', blob_value: 0x, null_value: 'present', tail_value: 'tail'}";

    private final QualifiedName tupleTable = uniqueTestTableFullName(TEST_KEYSPACE);
    private final QualifiedName frozenUdtTable = uniqueTestTableFullName(TEST_KEYSPACE);
    private final QualifiedName nonFrozenUdtTable = uniqueTestTableFullName(TEST_KEYSPACE);

    @Test
    public void testEmptyTupleComponents()
    {
        Row tuple = readCompositeValue(tupleTable);

        assertThat(tuple.getString(0)).isEmpty();
        assertThat((byte[]) tuple.get(1)).isEmpty();
        assertThat(tuple.isNullAt(2)).isTrue();
        assertThat(tuple.getString(3)).isEqualTo("tail");
    }

    @Test
    public void testEmptyFrozenUdtFields()
    {
        assertEmptyUdtFields(readCompositeValue(frozenUdtTable), true);
    }

    @Test
    public void testEmptyNonFrozenUdtFields()
    {
        assertEmptyUdtFields(readCompositeValue(nonFrozenUdtTable), false);
    }

    private Row readCompositeValue(QualifiedName table)
    {
        List<Row> rows = bulkReaderDataFrame(table).load().collectAsList();
        assertThat(rows).hasSize(1);

        Row value = rows.get(0).getStruct(rows.get(0).fieldIndex("data"));
        assertThat(value).isNotNull();

        return value;
    }

    private static void assertEmptyUdtFields(Row value, boolean frozen)
    {
        assertThat((String) value.getAs("text_value")).isEmpty();
        assertThat((byte[]) value.getAs("blob_value")).isEmpty();
        if (frozen)
        {
            assertThat(value.isNullAt(value.fieldIndex("null_value"))).isTrue();
        }
        else
        {
            // TODO: Remove once CASSANALYTICS-217 is fixed.
            assertThat((String) value.getAs("null_value")).isEqualTo("present");
        }
        assertThat((String) value.getAs("tail_value")).isEqualTo("tail");
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);

        cluster.schemaChangeIgnoringStoppedInstances(String.format("CREATE TYPE %s.%s (" +
                                                                   "text_value text, " +
                                                                   "blob_value blob, " +
                                                                   "null_value text, " +
                                                                   "tail_value text)",
                                                                   TEST_KEYSPACE, UDT_NAME));

        createTestTable(tupleTable, "CREATE TABLE %s (" +
                                    "id int PRIMARY KEY, " +
                                    "data frozen<tuple<text, blob, text, text>>)");

        createTestTable(frozenUdtTable, "CREATE TABLE %s (" +
                                        "id int PRIMARY KEY, " +
                                        "data frozen<" + UDT_NAME + ">)");

        createTestTable(nonFrozenUdtTable, "CREATE TABLE %s (" +
                                           "id int PRIMARY KEY, " +
                                           "data " + UDT_NAME + ")");

        IInstance node = cluster.getFirstRunningInstance();

        node.coordinator().execute(String.format("INSERT INTO %s (id, data) VALUES (1, ('', 0x, null, 'tail'))",
                                                 tupleTable),
                                   ConsistencyLevel.ALL);

        node.coordinator().execute(String.format("INSERT INTO %s (id, data) VALUES (1, %s)",
                                                 frozenUdtTable, EMPTY_FROZEN_UDT),
                                   ConsistencyLevel.ALL);

        node.coordinator().execute(String.format("INSERT INTO %s (id, data) VALUES (1, %s)",
                                                 nonFrozenUdtTable, EMPTY_NON_FROZEN_UDT),
                                   ConsistencyLevel.ALL);
    }
}
