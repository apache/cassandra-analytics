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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for SSTable version-based bridge selection in bulk writer (feature ENABLED)
 */
class BulkWriterSSTableVersionBridgeEnabledTest extends SharedClusterSparkIntegrationTestBase
{
    static final QualifiedName TABLE_ENABLED = new QualifiedName(TEST_KEYSPACE, "test_enabled");

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                    .nodesPerDc(3);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        cluster.schemaChangeIgnoringStoppedInstances(
            "CREATE TABLE " + TABLE_ENABLED + " (id int PRIMARY KEY, value text) WITH compression = {'enabled': false};"
        );
    }

    @Test
    void testBulkWriteWithSSTableVersionBasedBridgeEnabled()
    {
        SparkSession spark = getOrCreateSparkSession();

        // Create test data
        List<Row> data = Arrays.asList(
            RowFactory.create(0, "a"),
            RowFactory.create(1, "b"),
            RowFactory.create(2, "c"),
            RowFactory.create(3, "d"),
            RowFactory.create(4, "e")
        );

        StructType schema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("value", DataTypes.StringType, false)
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Write with SSTable version-based bridge enabled (default)
        bulkWriterDataFrameWriter(df, TABLE_ENABLED).save();

        // Verify data was written
        SimpleQueryResult result = cluster.coordinator(1)
                                          .executeWithResult("SELECT id, value FROM " + TABLE_ENABLED,
                                                           ConsistencyLevel.ALL);
        Object[][] rows = result.toObjectArrays();

        assertThat(rows.length).isEqualTo(data.size());

        // Verify all expected IDs are present
        List<Integer> actualIds = Arrays.stream(rows)
                                        .map(row -> (Integer) row[0])
                                        .sorted()
                                        .collect(java.util.stream.Collectors.toList());
        List<Integer> expectedIds = data.stream()
                                        .map(row -> row.getInt(0))
                                        .sorted()
                                        .collect(java.util.stream.Collectors.toList());
        assertThat(actualIds).isEqualTo(expectedIds);
    }
}
