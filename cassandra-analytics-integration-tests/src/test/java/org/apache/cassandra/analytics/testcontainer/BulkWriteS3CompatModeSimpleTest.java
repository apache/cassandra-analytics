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

package org.apache.cassandra.analytics.testcontainer;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import org.apache.cassandra.analytics.DataGenerationUtils;
import org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

class BulkWriteS3CompatModeSimpleTest extends SharedClusterSparkIntegrationTestBase
{
    public static final String BUCKET_NAME = "sbw-bucket";
    private static final QualifiedName TABLE_NAME = new QualifiedName(TEST_KEYSPACE, BulkWriteS3CompatModeSimpleTest.class.getSimpleName().toLowerCase());
    private S3MockContainer s3Mock;

    @Override
    protected void afterClusterProvisioned()
    {
        // must start s3Mock before starting sidecar, in order to provide the actual s3 server port to start sidecar
        super.afterClusterProvisioned();
        s3Mock = new S3MockContainer("2.17.0")
                 .withInitialBuckets(BUCKET_NAME);
        s3Mock.start();
        assertThat(s3Mock.isRunning()).isTrue();
    }

    @Override
    protected void afterClusterShutdown()
    {
        s3Mock.stop();
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
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        createTestTable(TABLE_NAME, CREATE_TEST_TABLE_STATEMENT);
    }

    @Override
    protected Map<String, String> sidecarAdditionalOptions()
    {
        return ImmutableMap.of(SIDECAR_S3_ENDPOINT_OVERRIDE_OPT, s3Mock.getHttpEndpoint());
    }

    /**
     * Write data using S3_COMPAT mode and read back using CQL. Assert that all written data are read back
     */
    @Test
    void testS3CompatBulkWrite()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
        Map<String, String> s3CompatOptions = ImmutableMap.of(
        "data_transport", "S3_COMPAT",
        "data_transport_extension_class", LocalStorageTransportExtension.class.getCanonicalName(),
        "storage_client_endpoint_override", s3Mock.getHttpEndpoint() // point to s3Mock server
        );
        bulkWriterDataFrameWriter(df, TABLE_NAME, s3CompatOptions).save();
        sparkTestUtils.validateWrites(df.collectAsList(), queryAllData(TABLE_NAME));
    }
}
