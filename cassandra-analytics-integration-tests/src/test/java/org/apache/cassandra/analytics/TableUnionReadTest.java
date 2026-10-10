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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Loads two tables with the same schema via the bulk reader and verifies Spark's
 * {@code Dataset#union} returns the combined rowset.
 */
class TableUnionReadTest extends SharedClusterSparkIntegrationTestBase
{
    QualifiedName table1 = uniqueTestTableFullName(TEST_KEYSPACE, "union1");
    QualifiedName table2 = uniqueTestTableFullName(TEST_KEYSPACE, "union2");

    static final List<Object[]> VALUES_1 = Arrays.asList(
        new Object[]{1L, "a"}, new Object[]{2L, "b"}, new Object[]{3L, "c"});
    static final List<Object[]> VALUES_2 = Arrays.asList(
        new Object[]{4L, "d"}, new Object[]{5L, "e"}, new Object[]{6L, "f"});

    @Test
    void testTableUnion()
    {
        Dataset<Row> data1 = bulkReaderDataFrame(table1).load();
        Dataset<Row> data2 = bulkReaderDataFrame(table2).load();
        Dataset<Row> union = data1.union(data2);

        Map<Long, String> expected = new HashMap<>();
        for (Object[] v : VALUES_1)
        {
            expected.put((Long) v[0], (String) v[1]);
        }

        for (Object[] v : VALUES_2)
        {
            expected.put((Long) v[0], (String) v[1]);
        }

        assertThat(union.count()).isEqualTo(expected.size());
        for (Row row : union.collectAsList())
        {
            long key = row.getLong(0);
            assertThat(expected).containsKey(key);
            assertThat(row.getString(1)).isEqualTo(expected.get(key));
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        String ddl = "CREATE TABLE IF NOT EXISTS %s (pk1 bigint, col1 text, PRIMARY KEY (pk1));";
        createTestTable(table1, ddl);
        createTestTable(table2, ddl);
        disableAutoCompaction(table1);

        for (Object[] v : VALUES_1)
        {
            execute(String.format("INSERT INTO %s (pk1, col1) VALUES (%d, '%s');", table1, (long) v[0], v[1]));
        }

        for (Object[] v : VALUES_2)
        {
            execute(String.format("INSERT INTO %s (pk1, col1) VALUES (%d, '%s');", table2, (long) v[0], v[1]));
        }

        flushKeyspace(table1);
    }
}
