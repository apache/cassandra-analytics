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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.vdurmont.semver4j.Semver;
import org.junit.jupiter.api.Test;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import org.apache.cassandra.analytics.DataGenerationUtils;
import org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.sidecar.config.S3ClientConfiguration;
import org.apache.cassandra.sidecar.config.S3ProxyConfiguration;
import org.apache.cassandra.sidecar.config.yaml.S3ClientConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.S3ProxyConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.TestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.sidecar.config.yaml.S3ClientConfigurationImpl.DEFAULT_API_CALL_TIMEOUT;
import static org.apache.cassandra.sidecar.config.yaml.S3ClientConfigurationImpl.DEFAULT_THREAD_KEEP_ALIVE;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * End-to-end integration test for bulk writing a table with Storage Attached Indexes (SAI) via the
 * S3-compatible (cloud storage / restore) transport.
 *
 * <p>This exercises both stages of the cloud-storage write path:
 * <ul>
 *   <li><b>Stage 1 (write &amp; upload):</b> SSTables and their SAI index components are produced on the Spark
 *       side by {@code CQLSSTableWriter}, bundled, and uploaded to S3. Because the restore payload now sets
 *       {@code failOnMissingIndex=true} for SAI tables, the subsequent import would fail if the SAI components
 *       were absent from the bundle — so a successful write proves the components were generated and uploaded.</li>
 *   <li><b>Stage 2 (restore &amp; import):</b> the sidecar restore job downloads the slices and runs Cassandra's
 *       SSTable import with {@code failOnMissingIndex=true}/{@code validateIndexChecksum=true}, attaching the SAI
 *       components. Successful completion, the presence of SAI files on disk, and working SAI-filtered queries
 *       confirm the indexes are usable after restore.</li>
 * </ul>
 *
 * SAI is a Cassandra 5.0+ feature, so the test is skipped on older clusters.
 */
class BulkWriteS3CompatModeSAIIndexTest extends SharedClusterSparkIntegrationTestBase
{
    // Must match the bucket reported by LocalStorageTransportExtension.getStorageConfiguration()
    // (it imports BUCKET_NAME from BulkWriteS3CompatModeSimpleTest = "sbw-bucket"); the writer uploads
    // bundles to that bucket, so the S3Mock initial bucket must use the same name or uploads 404.
    public static final String BUCKET_NAME = "sbw-bucket";
    private static final QualifiedName TABLE_NAME =
        new QualifiedName(TEST_KEYSPACE, BulkWriteS3CompatModeSAIIndexTest.class.getSimpleName().toLowerCase());
    private S3MockContainer s3Mock;

    /**
     * Stage 1 + Stage 2 end-to-end: write a SAI table via S3_COMPAT, then verify the data and the SAI indexes
     * survive the restore/import round-trip.
     */
    @Test
    void testS3CompatBulkWriteWithSaiIndexes()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
        Map<String, String> s3CompatOptions = ImmutableMap.of(
        "data_transport", "S3_COMPAT",
        "data_transport_extension_class", LocalStorageTransportExtension.class.getCanonicalName(),
        "storage_client_endpoint_override", s3Mock.getHttpEndpoint() // point to s3Mock server
        );

        // Stage 1 + Stage 2: write to S3 and restore/import. With failOnMissingIndex=true for SAI tables, this
        // save() would throw if the SAI components were not generated, uploaded, and validated on import.
        bulkWriterDataFrameWriter(df, TABLE_NAME, s3CompatOptions).save();

        // Verify all rows are present after the restore/import round-trip.
        sparkTestUtils.validateWrites(df.collectAsList(), queryAllData(TABLE_NAME));

        // Stage 2 evidence: SAI index components landed on disk after import.
        assertThat(hasSaiIndexFiles())
            .as("SAI index files should exist on disk after S3 restore/import")
            .isTrue();

        // Stage 2 evidence: SAI indexes are attached and usable for filtering on non-key columns.
        Object[][] courseResults = cluster.getFirstRunningInstance()
                                          .coordinator()
                                          .execute(String.format("SELECT * FROM %s WHERE course = 'course0';", TABLE_NAME),
                                                   ConsistencyLevel.ALL);
        assertThat(courseResults.length)
            .as("SAI filter on course='course0' should return the matching row")
            .isGreaterThan(0);
        for (Object[] row : courseResults)
        {
            // course is the second column (id, course, marks)
            assertThat(row[1]).isEqualTo("course0");
        }

        Object[][] marksResults = cluster.getFirstRunningInstance()
                                         .coordinator()
                                         .execute(String.format("SELECT * FROM %s WHERE marks = 50;", TABLE_NAME),
                                                  ConsistencyLevel.ALL);
        assertThat(marksResults.length)
            .as("SAI filter on marks=50 should return the matching row")
            .isGreaterThan(0);
        for (Object[] row : marksResults)
        {
            // marks is the third column (id, course, marks)
            assertThat(row[2]).isEqualTo(50);
        }
    }

    /**
     * Checks whether SAI index component files exist on disk for the test keyspace on the first node.
     */
    private boolean hasSaiIndexFiles()
    {
        String[] dataDirs = (String[]) cluster.get(1)
                                              .config()
                                              .getParams()
                                              .get("data_file_directories");
        Path keyspacePath = Paths.get(dataDirs[0], TEST_KEYSPACE);
        try (Stream<Path> walkStream = Files.walk(keyspacePath))
        {
            return walkStream
                   .filter(Files::isRegularFile)
                   .anyMatch(path -> {
                       String pathStr = path.toString();
                       return pathStr.contains("SAI") || pathStr.contains(".sai");
                   });
        }
        catch (IOException e)
        {
            return false;
        }
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        assumeThat(TestUtils.getDTestClusterVersion().isGreaterThanOrEqualTo(new Semver("5.0", Semver.SemverType.LOOSE)))
        .describedAs("Storage Attached Index (SAI) is only available in Cassandra 5.0 and above")
        .isTrue();
    }

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
        if (s3Mock != null)
        {
            s3Mock.stop();
        }
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
        cluster.schemaChangeIgnoringStoppedInstances(
            String.format("CREATE INDEX ON %s(course) USING 'StorageAttachedIndex';", TABLE_NAME));
        cluster.schemaChangeIgnoringStoppedInstances(
            String.format("CREATE INDEX ON %s(marks) USING 'StorageAttachedIndex';", TABLE_NAME));
    }

    @Override
    protected Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides()
    {
        return builder -> {
            S3ClientConfiguration s3ClientConfig = new S3ClientConfigurationImpl("s3-client", 4, DEFAULT_THREAD_KEEP_ALIVE,
                                                                                 5242880, DEFAULT_API_CALL_TIMEOUT,
                                                                                 buildTestS3ProxyConfig());
            builder.s3ClientConfiguration(s3ClientConfig);
            return builder;
        };
    }

    @Override
    protected void beforeTestStart()
    {
        super.beforeTestStart();
        waitForSchemaReady(30, TimeUnit.SECONDS);
    }

    private S3ProxyConfiguration buildTestS3ProxyConfig()
    {
        return new S3MockProxyConfigurationImpl(s3Mock.getHttpEndpoint());
    }

    public static class S3MockProxyConfigurationImpl extends S3ProxyConfigurationImpl
    {
        S3MockProxyConfigurationImpl(String endpointOverride)
        {
            super(null, null, null, endpointOverride);
        }
    }
}
