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

import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.acl.authorization.AllowAllAuthorizationProvider;
import org.apache.cassandra.sidecar.config.yaml.AccessControlConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ParameterizedClassConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.TestAuthenticationHandlerFactory;
import org.apache.cassandra.sidecar.testing.TestSidecarIdentityProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.cassandra.testing.TestUtils.CREATE_TEST_TABLE_STATEMENT;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.ROW_COUNT;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.assertj.core.api.Assertions.assertThat;

class SidecarIdentityProviderTest extends SharedClusterSparkIntegrationTestBase
{
    private static final QualifiedName TABLE_NAME = new QualifiedName(TEST_KEYSPACE, SidecarIdentityProviderTest.class.getSimpleName().toLowerCase());

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(TABLE_NAME, CREATE_TEST_TABLE_STATEMENT);
    }

    @Override
    protected Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides()
    {
        return builder -> {
            ParameterizedClassConfigurationImpl authenticator = new ParameterizedClassConfigurationImpl(TestAuthenticationHandlerFactory.class.getName(), null);
            ParameterizedClassConfigurationImpl authorizer = new ParameterizedClassConfigurationImpl(AllowAllAuthorizationProvider.class.getName(), null);
            builder.accessControlConfiguration(AccessControlConfigurationImpl.builder()
                                                                             .enabled(true)
                                                                             .authenticatorsConfiguration(ImmutableList.of(authenticator))
                                                                             .authorizerConfiguration(authorizer)
                                                                             .build());
            return builder;
        };
    }

    @Test
    void testCustomIdentityProvider()
    {
        Map<String, String> additionalOptions = ImmutableMap.of("sidecar_identity_provider_class",
                                                                TestSidecarIdentityProvider.class.getName());

        SparkSession spark = getOrCreateSparkSession();
        Dataset<Row> df = DataGenerationUtils.generateCourseData(spark, ROW_COUNT);
        bulkWriterDataFrameWriter(df, TABLE_NAME, additionalOptions).save();

        sparkTestUtils.validateWrites(df.collectAsList(), queryAllData(TABLE_NAME));

        Dataset<Row> read = bulkReaderDataFrame(TABLE_NAME, additionalOptions).load();
        assertThat(read.count()).isEqualTo(ROW_COUNT);
    }
}
