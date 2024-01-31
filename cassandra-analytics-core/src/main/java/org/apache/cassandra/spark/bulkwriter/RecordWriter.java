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
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.apache.commons.lang3.function.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.data.TimeSkewResponse;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.utils.DigestProvider;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.TaskContext;
import scala.Tuple2;

import static org.apache.cassandra.spark.utils.ScalaConversionUtils.asScalaIterator;

@SuppressWarnings({ "ConstantConditions" })
public class RecordWriter implements Serializable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordWriter.class);
    private static final long serialVersionUID = 3746578054834640428L;
    private final BulkWriterContext writerContext;
    private final String[] columnNames;
    private final TriFunction<BulkWriterContext, Path, DigestProvider, SSTableWriter> tableWriterSupplier;
    private final DigestProvider digestProvider;

    private final BulkWriteValidator writeValidator;
    private final ReplicaAwareFailureHandler<RingInstance> failureHandler;

    private final Supplier<TaskContext> taskContextSupplier;
    private SSTableWriter sstableWriter = null;
    private int outputSequence = 0; // sub-folder for possible subrange splits

    public RecordWriter(BulkWriterContext writerContext, String[] columnNames)
    {
        this(writerContext, columnNames, TaskContext::get, SSTableWriter::new);
    }

    @VisibleForTesting
    RecordWriter(BulkWriterContext writerContext,
                 String[] columnNames,
                 Supplier<TaskContext> taskContextSupplier,
                 TriFunction<BulkWriterContext, Path, DigestProvider, SSTableWriter> tableWriterSupplier)
    {
        this.writerContext = writerContext;
        this.columnNames = columnNames;
        this.taskContextSupplier = taskContextSupplier;
        this.tableWriterSupplier = tableWriterSupplier;
        this.failureHandler = new ReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner());
        this.writeValidator = new BulkWriteValidator(writerContext, failureHandler);
        this.digestProvider = this.writerContext.job().getDigestTypeOption().provider();

        writerContext.cluster().startupValidate();
    }

    private Range<BigInteger> getTokenRange(TaskContext taskContext)
    {
        return writerContext.job().getTokenPartitioner().getTokenRange(taskContext.partitionId());
    }

    private String getStreamId(TaskContext taskContext)
    {
        return String.format("%d-%s", taskContext.partitionId(), UUID.randomUUID());
    }

    public List<StreamResult> write(Iterator<Tuple2<DecoratedKey, Object[]>> sourceIterator)
    {
        TaskContext taskContext = taskContextSupplier.get();
        LOGGER.info("[{}]: Processing bulk writer partition", taskContext.partitionId());

        Range<BigInteger> taskTokenRange = getTokenRange(taskContext);
        Preconditions.checkState(!taskTokenRange.isEmpty(),
                                 "Token range for the partition %s is empty",
                                 taskTokenRange);

        TokenRangeMapping<RingInstance> initialTokenRangeMapping = writerContext.cluster().getTokenRangeMapping(false);
        LOGGER.info("[{}]: Fetched token range mapping for keyspace: {} with write replicas: {} containing pending " +
                    "replicas: {}, blocked instances: {}, replacement instances: {}",
                    taskContext.partitionId(),
                    writerContext.job().keyspace(),
                    initialTokenRangeMapping.getWriteReplicas().size(),
                    initialTokenRangeMapping.getPendingReplicas().size(),
                    initialTokenRangeMapping.getBlockedInstances().size(),
                    initialTokenRangeMapping.getReplacementInstances().size());

        Map<Range<BigInteger>, List<RingInstance>> initialTokenRangeInstances =
        taskTokenRangeMapping(initialTokenRangeMapping, taskTokenRange);
        List<StreamResult> results = new ArrayList<>();

        writeValidator.setPhase("Environment Validation");
        writeValidator.validateClOrFail(initialTokenRangeMapping);
        writeValidator.setPhase("UploadAndCommit");

        // for all replicas in this partition
        validateAcceptableTimeSkewOrThrow(new ArrayList<>(instancesFromMapping(initialTokenRangeInstances)));

        scala.collection.Iterator<scala.Tuple2<DecoratedKey, Object[]>> dataIterator =
        new InterruptibleIterator<>(taskContext, asScalaIterator(sourceIterator));
        StreamSession streamSession = null;
        int partitionId = taskContext.partitionId();
        JobInfo job = writerContext.job();
        Path baseDir = Paths.get(System.getProperty("java.io.tmpdir"),
                                 job.getId().toString(),
                                 Integer.toString(taskContext.stageAttemptNumber()),
                                 Integer.toString(taskContext.attemptNumber()),
                                 Integer.toString(partitionId));
        Map<String, Object> valueMap = new HashMap<>();
        try
        {
            // preserve the order of ranges
            Set<Range<BigInteger>> newRanges = new LinkedHashSet<>(initialTokenRangeMapping.getRangeMap()
                                                                                           .asMapOfRanges()
                                                                                           .keySet());
            Range<BigInteger> tokenRange = getTokenRange(taskContext);
            List<Range<BigInteger>> subRanges = newRanges.contains(tokenRange) ?
                                                Collections.singletonList(tokenRange) : // no overlaps
                                                getIntersectingSubRanges(newRanges, tokenRange); // has overlaps; split into sub-ranges

            int currentRangeIndex = 0;
            Range<BigInteger> currentRange = subRanges.get(currentRangeIndex);
            while (dataIterator.hasNext())
            {
                Tuple2<DecoratedKey, Object[]> rowData = dataIterator.next();
                BigInteger token = rowData._1().getToken();
                // Advance to the next range that contains the token.
                // The intermediate ranges that do not contain the token will be skipped
                while (!currentRange.contains(token))
                {
                    currentRangeIndex++;
                    if (currentRangeIndex >= subRanges.size())
                    {
                        String errMsg = String.format("Received Token %s outside the expected ranges %s", token, subRanges);
                        throw new IllegalStateException(errMsg);
                    }
                    currentRange = subRanges.get(currentRangeIndex);
                }
                streamSession = maybeCreateStreamSession(taskContext, streamSession, currentRange, failureHandler, results);
                maybeCreateTableWriter(partitionId, baseDir);
                writeRow(rowData, valueMap, partitionId, streamSession.getTokenRange());
            }

            // Finalize SSTable for the last StreamSession
            if (sstableWriter != null)
            {
                finalizeSSTable(streamSession, partitionId);
                results.add(streamSession.close());
            }
            LOGGER.info("[{}] Done with all writers and waiting for stream to complete", partitionId);

            // When instances for the partition's token range have changed within the scope of the task execution,
            // we fail the task for it to be retried
            validateTaskTokenRangeMappings(partitionId, initialTokenRangeMapping, taskTokenRange);
            return results;
        }
        catch (Exception exception)
        {
            LOGGER.error("[{}] Failed to write job={}, taskStageAttemptNumber={}, taskAttemptNumber={}",
                         partitionId,
                         job.getId().toString(),
                         taskContext.stageAttemptNumber(),
                         taskContext.attemptNumber());
            throw new RuntimeException(exception);
        }
    }

    private Map<Range<BigInteger>, List<RingInstance>> taskTokenRangeMapping(TokenRangeMapping<RingInstance> tokenRange,
                                                                             Range<BigInteger> taskTokenRange)
    {
        return tokenRange.getSubRanges(taskTokenRange).asMapOfRanges();
    }

    private Set<RingInstance> instancesFromMapping(Map<Range<BigInteger>, List<RingInstance>> mapping)
    {
        return mapping.values()
                      .stream()
                      .flatMap(Collection::stream)
                      .collect(Collectors.toSet());
    }

    /**
     * Creates a new session if we have the current token range intersecting the ranges from write replica-set.
     * If we do find the need to split a range into sub-ranges, we create the corresponding session for the sub-range
     * if the token from the row data belongs to the range.
     */
    private StreamSession maybeCreateStreamSession(TaskContext taskContext,
                                                   StreamSession streamSession,
                                                   Range<BigInteger> currentRange,
                                                   ReplicaAwareFailureHandler<RingInstance> failureHandler,
                                                   List<StreamResult> results)
    throws IOException, ExecutionException, InterruptedException
    {
        streamSession = maybeCreateSubRangeSession(taskContext, streamSession, failureHandler, results, currentRange);

        // If we do not have any stream session at this point, we create a session using the partition's token range
        return (streamSession == null) ? createStreamSession(taskContext) : streamSession;
    }

    /**
     * Given that the token belongs to a sub-range, creates a new stream session if either
     * 1) we do not have an existing stream session, or 2) the existing stream session corresponds to a range that
     * does NOT match the sub-range the token belongs to.
     */
    private StreamSession maybeCreateSubRangeSession(TaskContext taskContext,
                                                     StreamSession streamSession,
                                                     ReplicaAwareFailureHandler<RingInstance> failureHandler,
                                                     List<StreamResult> results,
                                                     Range<BigInteger> matchingSubRange)
    throws IOException, ExecutionException, InterruptedException
    {
        if (streamSession == null || streamSession.getTokenRange() != matchingSubRange)
        {
            LOGGER.debug("[{}] Creating stream session for range: {}", taskContext.partitionId(), matchingSubRange);
            // Schedule data to be sent if we are processing a batch that has not been scheduled yet.
            if (streamSession != null)
            {
                // Complete existing writes (if any) before the existing stream session is closed
                finalizeSSTable(streamSession, taskContext.partitionId());
                results.add(streamSession.close());
            }
            streamSession = new StreamSession(writerContext, getStreamId(taskContext), matchingSubRange, failureHandler);
        }
        return streamSession;
    }

    /**
     * Get ranges from the set that intersect and/or overlap with the provided token range
     */
    private List<Range<BigInteger>> getIntersectingSubRanges(Set<Range<BigInteger>> ranges, Range<BigInteger> tokenRange)
    {
        return ranges.stream()
                     .filter(r -> r.isConnected(tokenRange) && !r.intersection(tokenRange).isEmpty())
                     .collect(Collectors.toList());
    }

    private void validateTaskTokenRangeMappings(int partitionId,
                                                TokenRangeMapping<RingInstance> startTaskMapping,
                                                Range<BigInteger> taskTokenRange)
    {
        // Get the uncached, current view of the ring to compare with initial ring
        TokenRangeMapping<RingInstance> endTaskMapping = writerContext.cluster().getTokenRangeMapping(false);
        Map<Range<BigInteger>, List<RingInstance>> startMapping = taskTokenRangeMapping(startTaskMapping, taskTokenRange);
        Map<Range<BigInteger>, List<RingInstance>> endMapping = taskTokenRangeMapping(endTaskMapping, taskTokenRange);

        Set<RingInstance> initialInstances = instancesFromMapping(startMapping);
        Set<RingInstance> endInstances = instancesFromMapping(endMapping);
        // Token ranges are identical and overall instance list is same
        boolean haveMappingsChanged = !(startMapping.keySet().equals(endMapping.keySet()) &&
                                        initialInstances.equals(endInstances));
        if (haveMappingsChanged)
        {
            Set<Range<BigInteger>> rangeDelta = symmetricDifference(startMapping.keySet(), endMapping.keySet());
            Set<String> instanceDelta = symmetricDifference(initialInstances, endInstances).stream()
                                                                                           .map(RingInstance::ipAddress)
                                                                                           .collect(Collectors.toSet());
            String message = String.format("[%s] Token range mappings have changed since the task started " +
                                           "with non-overlapping instances: %s and ranges: %s",
                                           partitionId,
                                           instanceDelta,
                                           rangeDelta);
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
    }

    public static <T> Set<T> symmetricDifference(Set<T> set1, Set<T> set2)
    {
        return Stream.concat(
                     set1.stream().filter(element -> !set2.contains(element)),
                     set2.stream().filter(element -> !set1.contains(element)))
                     .collect(Collectors.toSet());
    }

    private void validateAcceptableTimeSkewOrThrow(List<RingInstance> replicas)
    {
        if (replicas.isEmpty())
        {
            return;
        }

        TimeSkewResponse timeSkewResponse = writerContext.cluster().getTimeSkew(replicas);
        Instant localNow = Instant.now();
        Instant remoteNow = Instant.ofEpochMilli(timeSkewResponse.currentTime);
        Duration range = Duration.ofMinutes(timeSkewResponse.allowableSkewInMinutes);
        if (localNow.isBefore(remoteNow.minus(range)) || localNow.isAfter(remoteNow.plus(range)))
        {
            final String message = String.format("Time skew between Spark and Cassandra is too large. "
                                                 + "Allowable skew is %d minutes. Spark executor time is %s, Cassandra instance time is %s",
                                                 timeSkewResponse.allowableSkewInMinutes, localNow, remoteNow);
            throw new UnsupportedOperationException(message);
        }
    }

    private void writeRow(Tuple2<DecoratedKey, Object[]> rowData,
                          Map<String, Object> valueMap,
                          int partitionId,
                          Range<BigInteger> range) throws IOException
    {
        DecoratedKey key = rowData._1();
        BigInteger token = key.getToken();
        Preconditions.checkState(range.contains(token),
                                 String.format("Received Token %s outside of expected range %s", token, range));
        try
        {
            sstableWriter.addRow(token, getBindValuesForColumns(valueMap, columnNames, rowData._2()));
        }
        catch (RuntimeException exception)
        {
            String message = String.format("[%s]: Failed to write data to SSTable: SBW DecoratedKey was %s",
                                           partitionId, key);
            LOGGER.error(message, exception);
            throw exception;
        }
    }

    private void maybeCreateTableWriter(int partitionId, Path baseDir) throws IOException
    {
        if (sstableWriter == null)
        {
            Path outDir = Paths.get(baseDir.toString(), Integer.toString(outputSequence++));
            Files.createDirectories(outDir);

            sstableWriter = tableWriterSupplier.apply(writerContext, outDir, digestProvider);
            LOGGER.info("[{}] Created new SSTable writer", partitionId);
        }
    }

    private static Map<String, Object> getBindValuesForColumns(Map<String, Object> map, String[] columnNames, Object[] values)
    {
        assert values.length == columnNames.length : "Number of values does not match the number of columns " + values.length + ", " + columnNames.length;
        for (int i = 0; i < columnNames.length; i++)
        {
            map.put(columnNames[i], values[i]);
        }
        return map;
    }

    /**
     * Close the {@link RecordWriter#sstableWriter} if present. Schedule a stream session with the produced sstables.
     * And finally, nullify {@link RecordWriter#sstableWriter}
     */
    private void finalizeSSTable(StreamSession streamSession,
                                 int partitionId) throws IOException
    {
        if (sstableWriter == null)
        {
            LOGGER.warn("SSTableWriter is null. Nothing to finalize");
            return;
        }
        LOGGER.info("[{}] Closing writer and scheduling SStable stream",
                    partitionId);
        sstableWriter.close(writerContext, partitionId);
        streamSession.scheduleStream(sstableWriter);
        sstableWriter = null;
    }

    private StreamSession createStreamSession(TaskContext taskContext)
    {
        Range<BigInteger> tokenRange = getTokenRange(taskContext);
        LOGGER.info("[{}] Creating stream session for range={}", taskContext.partitionId(), tokenRange);
        return new StreamSession(writerContext, getStreamId(taskContext), tokenRange, failureHandler);
    }
}
