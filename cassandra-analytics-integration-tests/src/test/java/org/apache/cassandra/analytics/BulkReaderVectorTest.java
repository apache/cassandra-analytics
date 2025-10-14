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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Tests bulk reader functionality
 */
class BulkReaderVectorTest extends SharedClusterSparkIntegrationTestBase
{
    static final int ROW_COUNT = 10;
    static final int DIMENSIONS = 3;
    static final List<List<Float>> DATASET = new ArrayList<>();
    static QualifiedName table1 = uniqueTestTableFullName(TEST_KEYSPACE);

    static
    {
        for (int i = 0; i < ROW_COUNT; i++)
        {
            List<Float> vector = new ArrayList<>();
            for (int j = 0; j < DIMENSIONS; j++)
            {
                vector.add(ThreadLocalRandom.current().nextFloat());
            }
            DATASET.add(vector);
        }
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        assumeThat(TestUtils.getDTestClusterVersion().isGreaterThanOrEqualTo(new Semver("5.0", Semver.SemverType.LOOSE)))
        .describedAs("Vector type was introduced in Cassandra 5.0")
        .isTrue();
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(2);
    }

    @Test
    void testReadingVectorColumn()
    {
        Dataset<Row> data = bulkReaderDataFrame(table1).load();

        List<Row> rows = data.collectAsList().stream()
                             .sorted(Comparator.comparing(row -> row.getInt(0)))
                             .collect(Collectors.toList());
        assertThat(rows.size()).isEqualTo(ROW_COUNT);

        for (int i = 0; i < ROW_COUNT; i++)
        {
            Row row = rows.get(i);
            List<Float> value = DATASET.get(i);
            assertThat(row.getList(1)).isEqualTo(value);
        }
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(table1, "CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, value vector<float, " + DIMENSIONS + ">);");

        IInstance firstRunningInstance = cluster.getFirstRunningInstance();
        for (int i = 0; i < ROW_COUNT; i++)
        {
            List<Float> value = DATASET.get(i);
            String query = String.format("INSERT INTO %s (id, value) VALUES (%d, %s);", table1, i, value.toString());
            firstRunningInstance.coordinator().execute(query, ConsistencyLevel.ALL);
        }
    }
}
