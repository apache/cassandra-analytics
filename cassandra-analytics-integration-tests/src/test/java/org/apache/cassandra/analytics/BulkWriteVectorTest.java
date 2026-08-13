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

import org.junit.jupiter.api.Test;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

public class BulkWriteVectorTest extends SharedClusterSparkIntegrationTestBase
{
    static final QualifiedName VECTOR_TABLE_NAME = new QualifiedName(TEST_KEYSPACE, "test_vector");
    public static final String VECTOR_TABLE_CREATE = "CREATE TABLE " + VECTOR_TABLE_NAME + " (\n"
                                                     + "          id BIGINT PRIMARY KEY,\n"
                                                     + "          value vector<FLOAT, 3>);";

    private ICoordinator coordinator;

    @Test
    void testVectorOfFloats()
    {
        int numRowsInserted = populateVectorOfFloats();
        // Create a spark frame with the data inserted during the setup
        Dataset<Row> sourceData = bulkReaderDataFrame(VECTOR_TABLE_NAME).load();
        assertThat(sourceData.count()).isEqualTo(numRowsInserted);

        // truncate table to re-insert the data
        truncateTable(VECTOR_TABLE_NAME);

        // Insert the dataset containing vectors
        bulkWriterDataFrameWriter(sourceData, VECTOR_TABLE_NAME).save();

        // Count rows because Java driver 3.x cannot read vector type
        assertThat(countDataWithDriver(VECTOR_TABLE_NAME)).isEqualTo(numRowsInserted);
    }

    private int populateVectorOfFloats()
    {
        String insert = "INSERT INTO %s (id, value) VALUES (%d, [%f, %f, %f])";

        int i = 0;
        for (; i < ROW_COUNT; i++)
        {
            float j = (float) i;
            cluster.schemaChangeIgnoringStoppedInstances(String.format(insert, VECTOR_TABLE_NAME,
                                                                       i, j, j, j));
        }

        // test null value
        coordinator.execute(String.format("insert into %s (id) values (%d)",
                                          VECTOR_TABLE_NAME, i++), ConsistencyLevel.ALL);

        return i;
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3);
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        assumeThat(TestUtils.getDTestClusterVersion().isGreaterThanOrEqualTo(new Semver("5.0", Semver.SemverType.LOOSE)))
        .describedAs("Vector type was introduced in Cassandra 5.0")
        .isTrue();
    }

    @Override
    protected void initializeSchemaForTest()
    {
        coordinator = cluster.getFirstRunningInstance().coordinator();

        createTestKeyspace(VECTOR_TABLE_NAME, DC1_RF3);

        cluster.schemaChangeIgnoringStoppedInstances(VECTOR_TABLE_CREATE);
    }

    private void truncateTable(QualifiedName tableName)
    {
        cluster.schemaChangeIgnoringStoppedInstances(String.format(
        "TRUNCATE %s.%s",
        TEST_KEYSPACE, tableName.table()));
    }
}
