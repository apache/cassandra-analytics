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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;

import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;

/**
 * Bulk writes partition and clustering keys of frozen collections, tuples, UDTs and vectors, nested in each other,
 * and a composite partition key whose components have different types
 */
class ComplexKeyTypesIntegrationTest extends KeyTypesIntegrationTestBase
{
    private static final QualifiedName COMPOSITE_SOURCE = new QualifiedName(TEST_KEYSPACE, "composite_source");
    private static final QualifiedName COMPOSITE_TARGET = new QualifiedName(TEST_KEYSPACE, "composite_target");
    private static final String CREATE_COMPOSITE_TABLE =
    "CREATE TABLE IF NOT EXISTS %s (pk_int int, pk_date date, pk_inet inet, pk_varint varint, pk_set frozen<set<text>>, "
    + "pk_udt frozen<simple_udt>, pk_tuple frozen<tuple<int, text>>, ck int, value int, "
    + "PRIMARY KEY ((pk_int, pk_date, pk_inet, pk_varint, pk_set, pk_udt, pk_tuple), ck)) WITH read_repair='NONE';";

    @Override
    List<KeyType> keyTypes()
    {
        List<KeyType> keyTypes = new ArrayList<>(Arrays.asList(
        new KeyType("frozen_list", "frozen<list<int>>", i -> String.format("[%d, 7, -1]", i)),
        new KeyType("frozen_set", "frozen<set<text>>", i -> String.format("{'banana_%d', 'apple', 'cherry'}", i)),
        new KeyType("frozen_map", "frozen<map<text, int>>", i -> String.format("{'banana_%d': %d, 'apple': 0}", i, i)),
        new KeyType("frozen_tuple", "frozen<tuple<int, text>>", i -> String.format("(%d, 'tuple')", i)),
        new KeyType("frozen_udt", "frozen<simple_udt>", i -> String.format("{a: %d, b: 'udt'}", i)),
        new KeyType("frozen_nested_udt", "frozen<nested_udt>",
                    i -> String.format("{id: %d, tags: ['banana', 'apple'], inner: {a: %d, b: 'inner'}}", i, -i)),
        new KeyType("frozen_list_of_tuples", "frozen<list<frozen<tuple<int, text>>>>",
                    i -> String.format("[(%d, 'a'), (0, 'b')]", i)),
        new KeyType("frozen_set_of_udts", "frozen<set<frozen<simple_udt>>>",
                    i -> String.format("{{a: %d, b: 'x'}, {a: -1, b: 'y'}}", i)),
        // The bulk reader fails with an AssertionError in RowIterator.next when a clustering key has this type,
        // so the type is only read and written as a partition key
        KeyType.partitionKeyOnly("frozen_map_of_udts", "frozen<map<int, frozen<simple_udt>>>",
                                 i -> String.format("{%d: {a: 1, b: 'x'}, -1: {a: 2, b: 'y'}}", i)),
        new KeyType("frozen_tuple_with_udt", "frozen<tuple<int, frozen<simple_udt>>>",
                    i -> String.format("(%d, {a: 1, b: 'x'})", i))));
        if (cassandraVersion().versionNumber() >= CassandraVersion.FIVEZERO.versionNumber())
        {
            keyTypes.add(new KeyType("vector", "vector<float, 3>", i -> String.format("[%d.5, 1.0, -2.0]", i)));
        }
        return keyTypes;
    }

    @Test
    void compositePartitionKeyOfDifferentTypes()
    {
        bulkWriteFromBulkRead(COMPOSITE_SOURCE, COMPOSITE_TARGET, ROW_COUNT);

        for (int i = 0; i < ROW_COUNT; i++)
        {
            assertValue(String.format("SELECT value FROM %s WHERE %s AND ck = %d", COMPOSITE_TARGET, compositeKey(i), i), i);
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        super.initializeSchemaForTest();
        createTestTable(COMPOSITE_SOURCE, CREATE_COMPOSITE_TABLE);
        createTestTable(COMPOSITE_TARGET, CREATE_COMPOSITE_TABLE);
        ICoordinator coordinator = cluster.getFirstRunningInstance().coordinator();
        for (int i = 0; i < ROW_COUNT; i++)
        {
            coordinator.execute(String.format("INSERT INTO %s (pk_int, pk_date, pk_inet, pk_varint, pk_set, pk_udt, pk_tuple, ck, value) "
                                              + "VALUES (%d, '2024-03-%02d', '%s', %s, {'banana_%d', 'apple'}, {a: %d, b: 'x'}, (%d, 'y'), %d, %d)",
                                              COMPOSITE_SOURCE, i % 2, i + 1, inet(i), varint(i), i, i, i, i, i),
                                ConsistencyLevel.ALL);
        }
    }

    private static String compositeKey(int i)
    {
        return String.format("pk_int = %d AND pk_date = '2024-03-%02d' AND pk_inet = '%s' AND pk_varint = %s "
                             + "AND pk_set = {'banana_%d', 'apple'} AND pk_udt = {a: %d, b: 'x'} AND pk_tuple = (%d, 'y')",
                             i % 2, i + 1, inet(i), varint(i), i, i, i);
    }

    private static String inet(int i)
    {
        return i % 2 == 0 ? "10.1.0." + i : "2001:db8::" + i;
    }

    private static String varint(int i)
    {
        return BigInteger.valueOf(i).multiply(BigInteger.TEN.pow(20)).toString();
    }
}
