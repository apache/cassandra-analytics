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

import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CassandraCoordinatedBulkWriterContext;
import org.apache.cassandra.spark.common.stats.JobStatsPublisher;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BulkWriterContextFactoryTest
{
    SparkContext sparkContext;
    Map<String, String> options = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    TestBulkWriterContextFactory testFactory = new TestBulkWriterContextFactory();
    StructType schema = mock(StructType.class);

    @BeforeEach
    void setup()
    {
        sparkContext = mock(SparkContext.class);
        when(sparkContext.defaultParallelism()).thenReturn(1);
        SparkConf conf = new SparkConf();
        when(sparkContext.getConf()).thenReturn(conf);
        options.put(WriterOptions.SIDECAR_CONTACT_POINTS.name(), "127.0.0.1");
        options.put(WriterOptions.KEYSPACE.name(), "ks");
        options.put(WriterOptions.TABLE.name(), "table");
        options.put(WriterOptions.KEYSTORE_PASSWORD.name(), "dummy_password");
        options.put(WriterOptions.KEYSTORE_PATH.name(), "dummy_path");
    }

    @Test
    void testCreateBulkWriterContext()
    {
        assertThat(testFactory.createBulkWriterContext(sparkContext, options, schema)).isInstanceOf(CassandraBulkWriterContext.class);
    }

    @Test
    void testCreateCoordinatedBulkWriterContext()
    {
        options.put(WriterOptions.DATA_TRANSPORT.name(), DataTransport.S3_COMPAT.name());
        options.put(WriterOptions.COORDINATED_WRITE_CONFIG.name(), "{\"cluster1\":{\"sidecarContactPoints\":[\"instance-1:9999\"]}}");
        assertThat(testFactory.createBulkWriterContext(sparkContext, options, schema)).isInstanceOf(CassandraCoordinatedBulkWriterContext.class);
    }

    static class TestBulkWriterContextFactory extends BulkWriterContextFactory
    {
        @NotNull
        @Override
        protected BulkWriterContext createCoordinatedBulkWriterContext(@NotNull BulkSparkConf conf, StructType schema, int sparkDefaultParallelism)
        {
            return setupMock(mock(CassandraCoordinatedBulkWriterContext.class));
        }

        @NotNull
        @Override
        protected BulkWriterContext createBulkWriterContext(@NotNull BulkSparkConf conf, StructType schema, int sparkDefaultParallelism)
        {
            return setupMock(mock(CassandraBulkWriterContext.class));
        }

        private BulkWriterContext setupMock(BulkWriterContext mock)
        {
            JobInfo job = mock(JobInfo.class);
            when(job.getId()).thenReturn("id");
            when(job.qualifiedTableName()).thenReturn(new QualifiedTableName("keyspace", "table"));
            when(mock.job()).thenReturn(job);
            when(mock.jobStats()).thenReturn(mock(JobStatsPublisher.class));
            return mock;
        }
    }
}
