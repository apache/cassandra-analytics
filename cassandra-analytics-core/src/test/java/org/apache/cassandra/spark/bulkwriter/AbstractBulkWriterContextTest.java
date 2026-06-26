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

package org.apache.cassandra.spark.bulkwriter;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterContainer;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractBulkWriterContextTest
{
    @Test
    void testKryoRegistrationWarningMessage()
    {
        // Test that the KRYO_REGISTRATION_WARNING constant exists and has expected content
        assertThat(AbstractBulkWriterContext.KRYO_REGISTRATION_WARNING)
        .isNotNull()
        .contains("Spark Bulk Writer Kryo Registrator")
        .contains("SbwKryoRegistrator")
        .contains("was not registered with Spark");
    }

    @Test
    void testContextUsesDeterminedBridgeVersion()
    {
        // The context's bridge version is whatever getBridgeVersion() resolves to. The determination logic
        // itself (override / SSTable-version-based / legacy) lives on CassandraClusterInfo and is covered by
        // CassandraClusterInfoBridgeVersionTest.
        BulkSparkConf conf = mock(BulkSparkConf.class);
        StructType schema = mock(StructType.class);
        TestBulkWriterContext context = TestBulkWriterContext.create(conf, schema, 1, "5.0.0");

        assertThat(context.bridgeVersion()).isEqualTo("5.0.0");
    }

    /**
     * Concrete test implementation of AbstractBulkWriterContext for testing. The bridge version is supplied
     * via the (mock) ClusterInfo returned by {@link #buildClusterInfo()} (the real determination is exercised
     * at the ClusterInfo level).
     */
    static class TestBulkWriterContext extends AbstractBulkWriterContext
    {
        private static String staticBridgeVersion;

        private TestBulkWriterContext(@NotNull BulkSparkConf conf,
                                      @NotNull StructType structType,
                                      int sparkDefaultParallelism)
        {
            super(conf, structType, sparkDefaultParallelism);
        }

        static TestBulkWriterContext create(@NotNull BulkSparkConf conf,
                                            @NotNull StructType structType,
                                            int sparkDefaultParallelism,
                                            String bridgeVersion)
        {
            staticBridgeVersion = bridgeVersion;
            return new TestBulkWriterContext(conf, structType, sparkDefaultParallelism);
        }

        @Override
        protected ClusterInfo buildClusterInfo()
        {
            ClusterInfo clusterInfo = mock(ClusterInfo.class);
            when(clusterInfo.getBridgeVersion()).thenReturn(staticBridgeVersion);
            return clusterInfo;
        }

        @Override
        protected JobInfo buildJobInfo()
        {
            // Return a mock JobInfo to avoid complex dependencies
            return mock(JobInfo.class);
        }

        @Override
        protected SchemaInfo buildSchemaInfo(StructType structType)
        {
            // Return a mock SchemaInfo to avoid complex dependencies
            return mock(SchemaInfo.class);
        }

        @Override
        protected TransportContext buildTransportContext(boolean isOnDriver)
        {
            // Return a mock TransportContext to avoid complex dependencies
            return mock(TransportContext.class);
        }

        @Override
        protected void validateKeyspaceReplication()
        {
            // No-op for testing
        }

        @Override
        protected MultiClusterContainer<UUID> generateRestoreJobIds()
        {
            return null;
        }

        @Override
        public BulkWriterConfig toBulkWriterConfigForBroadcasting(org.apache.spark.api.java.JavaSparkContext sparkContext)
        {
            throw new UnsupportedOperationException("Not needed for test");
        }
    }
}
