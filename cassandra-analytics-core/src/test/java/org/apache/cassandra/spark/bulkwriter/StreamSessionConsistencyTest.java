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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.cassandra.spark.bulkwriter.token.ConsistencyLevel;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.common.model.CassandraInstance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StreamSessionConsistencyTest
{
    private static final int NUMBER_DCS = 2;
    private static final int FILES_PER_SSTABLE = 8;
    private static final int REPLICATION_FACTOR = 3;
    private static final List<String> EXPECTED_INSTANCES = ImmutableList.of("DC1-i1", "DC1-i2", "DC1-i3", "DC2-i1", "DC2-i2", "DC2-i3");
    private static final Range<BigInteger> RANGE = Range.range(BigInteger.valueOf(101L), BoundType.CLOSED, BigInteger.valueOf(199L), BoundType.CLOSED);
    private static final TokenRangeMapping<RingInstance> TOKEN_RANGE_MAPPING =
    TokenRangeMappingUtils.buildTokenRangeMapping(0, ImmutableMap.of("DC1", 3, "DC2", 3), 6);
    private static final Map<String, Object> COLUMN_BIND_VALUES = ImmutableMap.of("id", 0, "date", 1, "course", "course", "marks", 2);

    @TempDir
    public Path folder; // CHECKSTYLE IGNORE: Public mutable field for testing
    private MockTableWriter tableWriter;
    private StreamSession streamSession;
    private MockBulkWriterContext writerContext;
    private final MockScheduledExecutorService executor = new MockScheduledExecutorService();

    public static Collection<Object[]> data()
    {
        List<ConsistencyLevel.CL> cls = Arrays.stream(ConsistencyLevel.CL.values()).collect(Collectors.toList());
        List<Integer> failures = IntStream.rangeClosed(0, REPLICATION_FACTOR).boxed().collect(Collectors.toList());
        List<List<Integer>> failuresPerDc = Lists.cartesianProduct(failures, failures);
        List<List<Object>> clsToFailures = Lists.cartesianProduct(cls, failuresPerDc);
        return clsToFailures.stream().map(List::toArray).collect(Collectors.toList());
    }

    private void setup(ConsistencyLevel.CL consistencyLevel, List<Integer> failuresPerDc)
    {
        tableWriter = new MockTableWriter(folder);
        writerContext = new MockBulkWriterContext(TOKEN_RANGE_MAPPING, "cassandra-4.0.0", consistencyLevel);
        streamSession = new StreamSession(writerContext,
                                          "sessionId",
                                          RANGE,
                                          executor,
                                          new ReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner()));
    }

    @ParameterizedTest(name = "CL: {0}, numFailures: {1}")
    @MethodSource("data")
    public void testConsistencyLevelAndFailureInCommit(ConsistencyLevel.CL consistencyLevel,
                                                       List<Integer> failuresPerDc)
    throws IOException, ExecutionException, InterruptedException
    {
        setup(consistencyLevel, failuresPerDc);
        streamSession = new StreamSession(writerContext,
                                          "sessionId",
                                          RANGE,
                                          executor,
                                          new ReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner()));

        AtomicInteger dc1Failures = new AtomicInteger(failuresPerDc.get(0));
        AtomicInteger dc2Failures = new AtomicInteger(failuresPerDc.get(1));
        ImmutableMap<String, AtomicInteger> dcFailures = ImmutableMap.of("DC1", dc1Failures, "DC2", dc2Failures);
        boolean shouldFail = calculateFailure(consistencyLevel, dc1Failures.get(), dc2Failures.get());
        // Return successful result for 1st result, failed for the rest
        writerContext.setCommitResultSupplier((uuids, dc) -> {
            if (dcFailures.get(dc).getAndDecrement() > 0)
            {
                return new DataTransferApi.RemoteCommitResult(false, uuids, Collections.emptyList(), "");
            }
            else
            {
                return new DataTransferApi.RemoteCommitResult(true, Collections.emptyList(), uuids, "");
            }
        });
        SSTableWriter tr = new NonValidatingTestSSTableWriter(tableWriter, folder);
        tr.addRow(BigInteger.valueOf(102L), COLUMN_BIND_VALUES);
        tr.close(writerContext, 1);
        streamSession.scheduleStream(tr);
        if (shouldFail)
        {
            RuntimeException exception = assertThrows(RuntimeException.class,
                                                      () -> streamSession.close());  // Force "execution" of futures
            assertEquals("Failed to load 1 ranges with " + consistencyLevel
                         + " for job " + writerContext.job().getId()
                         + " in phase UploadAndCommit.", exception.getMessage());
        }
        else
        {
            streamSession.close();  // Force "execution" of futures
        }
        executor.assertFuturesCalled();
        assertThat(writerContext.getUploads().values().stream()
                                .mapToInt(Collection::size)
                                .sum(),
                   equalTo(REPLICATION_FACTOR * NUMBER_DCS * FILES_PER_SSTABLE));
        List<String> instances = writerContext.getUploads().keySet().stream()
                                              .map(CassandraInstance::getNodeName)
                                              .collect(Collectors.toList());
        assertThat(instances, containsInAnyOrder(EXPECTED_INSTANCES.toArray()));
    }

    @ParameterizedTest(name = "CL: {0}, numFailures: {1}")
    @MethodSource("data")
    public void testConsistencyLevelAndFailureInUpload(ConsistencyLevel.CL consistencyLevel,
                                                       List<Integer> failuresPerDc)
    throws IOException, ExecutionException, InterruptedException
    {
        setup(consistencyLevel, failuresPerDc);
        streamSession = new StreamSession(writerContext,
                                          "sessionId",
                                          RANGE,
                                          executor,
                                          new ReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner()));
        AtomicInteger dc1Failures = new AtomicInteger(failuresPerDc.get(0));
        AtomicInteger dc2Failures = new AtomicInteger(failuresPerDc.get(1));
        int numFailures = dc1Failures.get() + dc2Failures.get();
        ImmutableMap<String, AtomicInteger> dcFailures = ImmutableMap.of("DC1", dc1Failures, "DC2", dc2Failures);
        boolean shouldFail = calculateFailure(consistencyLevel, dc1Failures.get(), dc2Failures.get());
        writerContext.setUploadSupplier(instance -> dcFailures.get(instance.getDataCenter()).getAndDecrement() <= 0);
        SSTableWriter tr = new NonValidatingTestSSTableWriter(tableWriter, folder);
        tr.addRow(BigInteger.valueOf(102L), COLUMN_BIND_VALUES);
        tr.close(writerContext, 1);
        streamSession.scheduleStream(tr);
        if (shouldFail)
        {
            RuntimeException exception = assertThrows(RuntimeException.class,
                                                      () -> streamSession.close());  // Force "execution" of futures
            assertEquals("Failed to load 1 ranges with " + consistencyLevel
                         + " for job " + writerContext.job().getId()
                         + " in phase UploadAndCommit.", exception.getMessage());
        }
        else
        {
            streamSession.close();  // Force "execution" of futures
        }
        executor.assertFuturesCalled();
        int totalFilesToUpload = REPLICATION_FACTOR * NUMBER_DCS * FILES_PER_SSTABLE;
        // Once a file fails to upload, the rest of the components are not attempted
        int filesSkipped = (numFailures * (FILES_PER_SSTABLE - 1));
        assertThat(writerContext.getUploads().values().stream()
                                .mapToInt(Collection::size)
                                .sum(),
                   equalTo(totalFilesToUpload - filesSkipped));
        List<String> instances = writerContext.getUploads().keySet().stream()
                                              .map(CassandraInstance::getNodeName)
                                              .collect(Collectors.toList());
        assertThat(instances, containsInAnyOrder(EXPECTED_INSTANCES.toArray()));
    }

    private boolean calculateFailure(ConsistencyLevel.CL consistencyLevel, int dc1Failures, int dc2Failures)
    {
        // Assumes LOCAL_DC is DC1, given current CL and Failures, should we fail?
        int localQuorum = REPLICATION_FACTOR / 2 + 1;
        int localFailuresViolatingQuorum = REPLICATION_FACTOR - localQuorum;
        int totalInstances = NUMBER_DCS * REPLICATION_FACTOR;
        int allDcsQuorum = totalInstances / 2 + 1;
        switch (consistencyLevel)
        {
            case ALL:
                return dc1Failures + dc2Failures > 0;
            case TWO:
                return dc1Failures + dc2Failures > (totalInstances - 2);
            case QUORUM:
                return dc1Failures + dc2Failures > totalInstances - allDcsQuorum;  // Total instances - quorum
            case LOCAL_QUORUM:
                return (dc1Failures > localFailuresViolatingQuorum);
            case EACH_QUORUM:
                return (dc1Failures > localFailuresViolatingQuorum) || (dc2Failures > localFailuresViolatingQuorum);
            case LOCAL_ONE:
                return dc1Failures > REPLICATION_FACTOR - 1;
            case ONE:
                return (dc1Failures + dc2Failures) > totalInstances - 1;
            default:
                throw new IllegalArgumentException("CL: " + consistencyLevel + " not supported");
        }
    }
}
