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

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.MultiClusterContainer;
import org.apache.cassandra.spark.common.stats.JobStatsPublisher;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that verify the extensibility contract for the bulk writer broadcast/reconstruction chain.
 * These tests prove that downstream implementations can:
 * <ul>
 *   <li>Subclass {@link BulkWriterConfig} and override {@link BulkWriterConfig#toBulkWriterContext()}</li>
 *   <li>Implement {@link IBroadcastableClusterInfo} with custom {@code reconstruct()} logic</li>
 *   <li>Subclass {@link AbstractBulkWriterContext} and override {@code reconstructJobInfoOnExecutor()}</li>
 * </ul>
 */
class BulkWriterConfigExtensibilityTest
{
    @Test
    void testToBulkWriterContextCanBeOverridden()
    {
        BulkSparkConf mockConf = mock(BulkSparkConf.class);
        BroadcastableJobInfo mockJobInfo = mock(BroadcastableJobInfo.class);
        IBroadcastableClusterInfo mockClusterInfo = mock(IBroadcastableClusterInfo.class);
        BroadcastableSchemaInfo mockSchemaInfo = mock(BroadcastableSchemaInfo.class);

        // A custom BulkWriterConfig subclass overriding toBulkWriterContext()
        BulkWriterConfig customConfig = new BulkWriterConfig(mockConf, 4, mockJobInfo, mockClusterInfo, mockSchemaInfo, "4.0.0")
        {
            @Override
            public BulkWriterContext toBulkWriterContext()
            {
                return mock(BulkWriterContext.class);
            }
        };

        BulkWriterContext context = customConfig.toBulkWriterContext();
        assertThat(context).isNotNull();
        // The base class would return CassandraBulkWriterContext or CassandraCoordinatedBulkWriterContext,
        // but our subclass returns a mock — proving the override is dispatched.
        assertThat(context).isNotInstanceOf(CassandraBulkWriterContext.class);
    }

    @Test
    void testCustomIBroadcastableClusterInfoReconstructIsCalled()
    {
        ClusterInfo expectedCluster = mock(ClusterInfo.class);

        // Custom IBroadcastableClusterInfo whose reconstruct() returns a specific ClusterInfo
        IBroadcastableClusterInfo customBroadcastable = new IBroadcastableClusterInfo()
        {
            @Override
            public Partitioner getPartitioner()
            {
                return Partitioner.Murmur3Partitioner;
            }

            @Override
            public String getLowestCassandraVersion()
            {
                return "4.0.0";
            }

            @Nullable
            @Override
            public String clusterId()
            {
                return null;
            }

            @NotNull
            @Override
            public BulkSparkConf getConf()
            {
                return mock(BulkSparkConf.class);
            }

            @Override
            public ClusterInfo reconstruct()
            {
                return expectedCluster;
            }
        };

        BulkSparkConf mockConf = mock(BulkSparkConf.class);
        BroadcastableJobInfo mockJobInfo = mock(BroadcastableJobInfo.class);
        when(mockJobInfo.getConf()).thenReturn(mockConf);
        when(mockJobInfo.getRestoreJobIds()).thenReturn(MultiClusterContainer.ofSingle(UUID.randomUUID()));
        BroadcastableSchemaInfo mockSchemaInfo = mock(BroadcastableSchemaInfo.class);

        BulkWriterConfig config = new BulkWriterConfig(mockConf, 4, mockJobInfo, customBroadcastable, mockSchemaInfo, "4.0.0");

        // Use a test subclass that overrides expensive methods to avoid needing real infrastructure
        TestBulkWriterContext context = new TestBulkWriterContext(config);

        assertThat(context.cluster()).isSameAs(expectedCluster);
    }

    @Test
    void testReconstructJobInfoOnExecutorCanBeOverridden()
    {
        JobInfo expectedJobInfo = mock(JobInfo.class);
        ClusterInfo mockCluster = mock(ClusterInfo.class);

        IBroadcastableClusterInfo customBroadcastable = new IBroadcastableClusterInfo()
        {
            @Override
            public Partitioner getPartitioner()
            {
                return Partitioner.Murmur3Partitioner;
            }

            @Override
            public String getLowestCassandraVersion()
            {
                return "4.0.0";
            }

            @Nullable
            @Override
            public String clusterId()
            {
                return null;
            }

            @NotNull
            @Override
            public BulkSparkConf getConf()
            {
                return mock(BulkSparkConf.class);
            }

            @Override
            public ClusterInfo reconstruct()
            {
                return mockCluster;
            }
        };

        BulkSparkConf mockConf = mock(BulkSparkConf.class);
        BroadcastableJobInfo mockJobInfo = mock(BroadcastableJobInfo.class);
        when(mockJobInfo.getConf()).thenReturn(mockConf);
        when(mockJobInfo.getRestoreJobIds()).thenReturn(MultiClusterContainer.ofSingle(UUID.randomUUID()));
        BroadcastableSchemaInfo mockSchemaInfo = mock(BroadcastableSchemaInfo.class);

        BulkWriterConfig config = new BulkWriterConfig(mockConf, 4, mockJobInfo, customBroadcastable, mockSchemaInfo, "4.0.0");

        // Subclass that overrides reconstructJobInfoOnExecutor to return custom JobInfo
        TestBulkWriterContext context = new TestBulkWriterContext(config)
        {
            @Override
            protected JobInfo reconstructJobInfoOnExecutor(BroadcastableJobInfo jobInfo)
            {
                return expectedJobInfo;
            }
        };

        assertThat(context.job()).isSameAs(expectedJobInfo);
    }

    /**
     * Minimal AbstractBulkWriterContext subclass for testing executor-side reconstruction
     * without requiring real Cassandra infrastructure.
     */
    private static class TestBulkWriterContext extends AbstractBulkWriterContext
    {
        TestBulkWriterContext(@NotNull BulkWriterConfig config)
        {
            super(config);
        }

        @Override
        protected ClusterInfo buildClusterInfo()
        {
            throw new UnsupportedOperationException("Driver-only");
        }

        @Override
        protected void validateKeyspaceReplication()
        {
        }

        @Override
        protected MultiClusterContainer<UUID> generateRestoreJobIds()
        {
            throw new UnsupportedOperationException("Driver-only");
        }

        @Override
        protected CassandraBridge buildCassandraBridge()
        {
            return mock(CassandraBridge.class);
        }

        @Override
        protected TransportContext buildTransportContext(boolean isOnDriver)
        {
            return mock(TransportContext.class);
        }

        @Override
        protected JobStatsPublisher buildJobStatsPublisher()
        {
            return mock(JobStatsPublisher.class);
        }

        @Override
        protected JobInfo reconstructJobInfoOnExecutor(BroadcastableJobInfo jobInfo)
        {
            return mock(JobInfo.class);
        }

        @Override
        protected SchemaInfo reconstructSchemaInfoOnExecutor(BroadcastableSchemaInfo schemaInfo)
        {
            return mock(SchemaInfo.class);
        }

        @Override
        public BulkWriterConfig toBulkWriterConfigForBroadcasting(org.apache.spark.api.java.JavaSparkContext sparkContext)
        {
            throw new UnsupportedOperationException("Not needed for test");
        }
    }
}
