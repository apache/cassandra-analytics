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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.cassandra.testing.TestVersion;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.analytics.SparkTestUtils.validateWrites;
import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.uniqueTestTableFullName;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Tests bulk writes with different digest options
 */
class WriterDigestIntegrationTest extends SharedClusterSparkIntegrationTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WriterDigestIntegrationTest.class);
    static final QualifiedName DEFAULT_DIGEST_TABLE = uniqueTestTableFullName("default_digest");
    static final QualifiedName MD5_DIGEST_TABLE = uniqueTestTableFullName("md5_digest");
    static final QualifiedName CORRUPT_SSTABLE_TABLE = uniqueTestTableFullName("corrupt_sstable");
    static final List<QualifiedName> TABLE_NAMES = Arrays.asList(DEFAULT_DIGEST_TABLE, MD5_DIGEST_TABLE,
                                                                 CORRUPT_SSTABLE_TABLE);
    Dataset<Row> df;

    @Test
    void testDefaultDigest()
    {
        bulkWriterDataFrameWriter(df, DEFAULT_DIGEST_TABLE).save();
        validateWrites(cluster, DEFAULT_DIGEST_TABLE, df);
    }

    @Test
    void testMD5Digest()
    {
        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
        bulkWriterDataFrameWriter(df, MD5_DIGEST_TABLE).option(WriterOptions.DIGEST_TYPE.name(), "MD5").save();
        validateWrites(cluster, DEFAULT_DIGEST_TABLE, df);
    }

    @Test
    void failsOnInvalidDigestOption()
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> bulkWriterDataFrameWriter(df, DEFAULT_DIGEST_TABLE).option(WriterOptions.DIGEST_TYPE.name(), "invalid")
                                                                             .save())
        .withMessageContaining("Key digest type with value invalid is not a valid Enum of type class org.apache.cassandra.spark.bulkwriter.DigestTypeOption");
    }

    @Override
    protected void beforeTestStart()
    {
        SparkSession spark = getOrCreateSparkSession();
        df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
    }

    @Override
    protected UpgradeableCluster provisionCluster(TestVersion testVersion) throws IOException
    {
        // spin up a C* cluster using the in-jvm dtest
        Versions versions = Versions.find();
        Versions.Version requestedVersion = versions.getLatest(new Semver(testVersion.version(), Semver.SemverType.LOOSE));

        UpgradeableCluster.Builder clusterBuilder =
        UpgradeableCluster.build(1)
                          .withDynamicPortAllocation(true)
                          .withVersion(requestedVersion)
                          .withDCs(1)
                          .withDataDirCount(1)
                          .withConfig(config -> config.with(Feature.NATIVE_PROTOCOL)
                                                      .with(Feature.GOSSIP)
                                                      .with(Feature.JMX));
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(1, clusterBuilder.getTokenCount());
        clusterBuilder.withTokenSupplier(tokenSupplier);
        UpgradeableCluster cluster = clusterBuilder.createWithoutStarting();
        cluster.startup();
        return cluster;
    }

    @Override
    protected void initializeSchemaForTest()
    {
        TABLE_NAMES.forEach(name -> {
            createTestKeyspace(name, DC1_RF1);
            createTestTable(name, CREATE_TEST_TABLE_STATEMENT);
        });
    }
}
