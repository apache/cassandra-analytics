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

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.spark.bulkwriter.token.MultiClusterReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.utils.XXHash32DigestAlgorithm;

import static org.apache.cassandra.spark.data.ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link CassandraDirectDataTransportContext#createStreamSession}.
 */
public class CassandraDirectDataTransportContextTest
{
    @TempDir
    private Path folder;

    private MockBulkWriterContext writerContext;
    private MockTableWriter tableWriter;
    private ExecutorService executor;
    private Range<BigInteger> range;

    @BeforeEach
    public void setup()
    {
        ImmutableMap<String, Integer> rfOptions = ImmutableMap.of("DC1", 3);
        ReplicationFactor rf = new ReplicationFactor(NetworkTopologyStrategy, rfOptions);
        TokenRangeMapping<RingInstance> tokenRangeMapping = TokenRangeMappingUtils.buildTokenRangeMapping(0, rfOptions, 12);
        writerContext = new MockBulkWriterContext(tokenRangeMapping);
        writerContext.setReplicationFactor(rf);
        tableWriter = new MockTableWriter(folder);
        range = Range.range(BigInteger.valueOf(101L), BoundType.CLOSED, BigInteger.valueOf(199L), BoundType.CLOSED);
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    void createDirectStreamSessionForUntrackedKeyspace()
    {
        writerContext.setTrackedKeyspace(false);
        CassandraDirectDataTransportContext transportContext = stubTransportContext();

        StreamSession<?> session = transportContext.createStreamSession(
        writerContext,
        "session-untracked",
        new SortedSSTableWriter(tableWriter, folder, new XXHash32DigestAlgorithm(), 1),
        range,
        buildFailureHandler(),
        executor);

        assertThat(session)
        .describedAs("Untracked keyspace should produce a DirectStreamSession")
        .isInstanceOf(DirectStreamSession.class);
    }

    @Test
    void createTrackedDirectStreamSessionForTrackedKeyspace()
    {
        writerContext.setTrackedKeyspace(true);
        CassandraDirectDataTransportContext transportContext = stubTransportContext();

        StreamSession<?> session = transportContext.createStreamSession(
        writerContext,
        "session-tracked",
        new SortedSSTableWriter(tableWriter, folder, new XXHash32DigestAlgorithm(), 1),
        range,
        buildFailureHandler(),
        executor);

        assertThat(session)
        .describedAs("Tracked keyspace should produce a TrackedDirectStreamSession")
        .isInstanceOf(TrackedDirectStreamSession.class);
    }

    @Test
    void createDirectStreamSessionForAbsentReplicationType()
    {
        CassandraDirectDataTransportContext transportContext = stubTransportContext();

        StreamSession<?> session = transportContext.createStreamSession(
        writerContext,
        "session-null-type",
        new SortedSSTableWriter(tableWriter, folder, new XXHash32DigestAlgorithm(), 1),
        range,
        buildFailureHandler(),
        executor);

        assertThat(session)
        .describedAs("Null replication type (pre-mutation-tracking) should produce a DirectStreamSession")
        .isInstanceOf(DirectStreamSession.class);
    }

    private CassandraDirectDataTransportContext stubTransportContext()
    {
        DirectDataTransferApi mockApi =
        ((TransportContext.DirectDataBulkWriterContext) writerContext.transportContext()).dataTransferApi();

        return new CassandraDirectDataTransportContext(writerContext)
        {
            @Override
            protected DirectDataTransferApi createDirectDataTransferApi()
            {
                return mockApi;
            }
        };
    }

    private ReplicaAwareFailureHandler<RingInstance> buildFailureHandler()
    {
        return new MultiClusterReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner());
    }
}
