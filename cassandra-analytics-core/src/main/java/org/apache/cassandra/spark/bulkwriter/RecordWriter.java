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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.token.MultiClusterReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.bulkwriter.token.TokenRangeMapping;
import org.apache.cassandra.spark.bulkwriter.util.TaskContextUtils;
import org.apache.cassandra.spark.data.BridgeUdtValue;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.utils.DigestAlgorithm;
import org.apache.cassandra.util.ThreadUtil;
import org.apache.spark.TaskContext;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

@SuppressWarnings({ "ConstantConditions" })
public class RecordWriter
{
    public static final ReplicationFactor IGNORED_REPLICATION_FACTOR = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                                                                             ImmutableMap.of("replication_factor", 1));
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordWriter.class);

    private final BulkWriterContext writerContext;
    private final String[] columnNames;
    private final SSTableWriterFactory tableWriterFactory;
    private final DigestAlgorithm digestAlgorithm;
    private final BulkWriteValidator writeValidator;
    private final ReplicaAwareFailureHandler<RingInstance> failureHandler;
    private final Supplier<TaskContext> taskContextSupplier;
    private final ConcurrentHashMap<String, CqlField.CqlUdt> udtCache = new ConcurrentHashMap<>();
    private final Map<String, Future<StreamResult>> streamFutures;
    private final ExecutorService executorService;
    private final Path baseDir;

    private volatile CqlTable cqlTable;
    private StreamSession<?> streamSession = null;

    public RecordWriter(BulkWriterContext writerContext, String[] columnNames)
    {
        this(writerContext, columnNames, TaskContext::get, SortedSSTableWriter::new);
    }

    @VisibleForTesting
    RecordWriter(BulkWriterContext writerContext,
                 String[] columnNames,
                 Supplier<TaskContext> taskContextSupplier,
                 SSTableWriterFactory tableWriterFactory)
    {
        this.writerContext = writerContext;
        this.columnNames = columnNames;
        this.taskContextSupplier = taskContextSupplier;
        this.tableWriterFactory = tableWriterFactory;
        this.failureHandler = new MultiClusterReplicaAwareFailureHandler<>(writerContext.cluster().getPartitioner());
        this.writeValidator = new BulkWriteValidator(writerContext, failureHandler);
        this.digestAlgorithm = this.writerContext.job().digestAlgorithmSupplier().get();
        this.streamFutures = new HashMap<>();
        this.executorService = Executors.newSingleThreadExecutor(ThreadUtil.threadFactory("RecordWriter-worker"));
        this.baseDir = TaskContextUtils.getPartitionUniquePath(System.getProperty("java.io.tmpdir"),
                                                               writerContext.job().getId(),
                                                               taskContextSupplier.get());

        writerContext.cluster().startupValidate();
    }

    private CqlTable cqlTable()
    {
        if (cqlTable == null)
        {
            cqlTable = writerContext.bridge()
                                    .buildSchema(writerContext.schema().getTableSchema().createStatement,
                                                 writerContext.job().qualifiedTableName().keyspace(),
                                                 IGNORED_REPLICATION_FACTOR,
                                                 writerContext.cluster().getPartitioner(),
                                                 writerContext.schema().getUserDefinedTypeStatements());
        }

        return cqlTable;
    }

    /**
     * Write data into stream
     * @param sourceIterator source data
     * @return write result
     */
    public WriteResult write(Iterator<Tuple2<DecoratedKey, Object[]>> sourceIterator)
    {
        TaskContext taskContext = taskContextSupplier.get();
        LOGGER.info("[{}]: Processing bulk writer partition", taskContext.partitionId());

        Range<BigInteger> taskTokenRange = getTokenRange(taskContext);
        Preconditions.checkState(!taskTokenRange.isEmpty(),
                                 "Token range for the partition %s is empty",
                                 taskTokenRange);

        TokenRangeMapping<RingInstance> initialTokenRangeMapping = writerContext.cluster().getTokenRangeMapping(false);
        boolean isClusterBeingResized = !initialTokenRangeMapping.pendingInstances().isEmpty();
        LOGGER.info("[{}]: Fetched token range mapping for keyspace: {} with write instances: {} " +
                    "containing pending instances: {}",
                    taskContext.partitionId(),
                    writerContext.job().qualifiedTableName().keyspace(),
                    initialTokenRangeMapping.allInstances().size(),
                    initialTokenRangeMapping.pendingInstances().size());

        writeValidator.setPhase("Environment Validation");
        writeValidator.validateClOrFail(initialTokenRangeMapping);
        writeValidator.setPhase("UploadAndCommit");
        writerContext.cluster().validateTimeSkew(taskTokenRange);

        Iterator<Tuple2<DecoratedKey, Object[]>> dataIterator = new JavaInterruptibleIterator<>(taskContext, sourceIterator);
        int partitionId = taskContext.partitionId();
        JobInfo job = writerContext.job();
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
                if (streamSession != null)
                {
                    streamSession.throwIfLastStreamFailed();
                }
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
                maybeSwitchToNewStreamSession(taskContext, currentRange);
                writeRow(rowData, valueMap, partitionId, streamSession.getTokenRange());
            }

            // Finalize SSTable for the last StreamSession
            if (streamSession != null)
            {
                flushAsync(partitionId);
            }

            List<StreamResult> results = waitForStreamCompletionAndValidate(partitionId, initialTokenRangeMapping, taskTokenRange);
            return new WriteResult(results, isClusterBeingResized);
        }
        catch (Exception exception)
        {
            LOGGER.error("[{}] Failed to write job={}, taskStageAttemptNumber={}, taskAttemptNumber={}",
                         partitionId,
                         job.getId(),
                         taskContext.stageAttemptNumber(),
                         taskContext.attemptNumber(),
                         exception);

            if (exception instanceof InterruptedException)
            {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(exception);
        }
    }

    @NotNull
    private List<StreamResult> waitForStreamCompletionAndValidate(int partitionId,
                                                                  TokenRangeMapping<RingInstance> initialTokenRangeMapping,
                                                                  Range<BigInteger> taskTokenRange)
    {
        List<StreamResult> results = streamFutures.values().stream().map(f -> {
            try
            {
                return f.get();
            }
            catch (Exception e)
            {
                if (e instanceof InterruptedException)
                {
                    Thread.currentThread().interrupt();
                }
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        LOGGER.info("[{}] Done with all writers and waiting for stream to complete", partitionId);

        // When instances for the partition's token range have changed within the scope of the task execution,
        // we fail the task for it to be retried
        validateTaskTokenRangeMappings(partitionId, initialTokenRangeMapping, taskTokenRange);
        return results;
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
    private void maybeSwitchToNewStreamSession(TaskContext taskContext,
                                               Range<BigInteger> currentRange) throws IOException
    {
        if (streamSession != null && streamSession.getTokenRange().equals(currentRange))
        {
            return;
        }

        // Schedule data to be sent if we are processing a batch that has not been scheduled yet.
        if (streamSession != null)
        {
            // Complete existing writes (if any) before the existing stream session is closed
            flushAsync(taskContext.partitionId());
        }

        streamSession = createStreamSession(taskContext, currentRange);
    }

    private StreamSession<?> createStreamSession(TaskContext taskContext, Range<BigInteger> range) throws IOException
    {
        LOGGER.info("[{}] Creating new stream session. range={}", taskContext.partitionId(), range);

        String sessionId = TaskContextUtils.createStreamSessionId(taskContext);
        Path perSessionDirectory = baseDir.resolve(sessionId);
        Files.createDirectories(perSessionDirectory);
        SortedSSTableWriter sstableWriter = tableWriterFactory.create(writerContext, perSessionDirectory, digestAlgorithm, taskContext.partitionId());
        LOGGER.info("[{}][{}] Created new SSTable writer with directory={}",
                    taskContext.partitionId(), sessionId, perSessionDirectory);
        return writerContext.transportContext()
                            .createStreamSession(writerContext, sessionId, sstableWriter, range, failureHandler, executorService);
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
            Set<String> instanceDelta = symmetricDifference(initialInstances, endInstances)
                                        .stream()
                                        .map(RingInstance::ipAddressWithPort)
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

    static <T> Set<T> symmetricDifference(Set<T> set1, Set<T> set2)
    {
        return Stream.concat(
                     set1.stream().filter(element -> !set2.contains(element)),
                     set2.stream().filter(element -> !set1.contains(element)))
                     .collect(Collectors.toSet());
    }

    private Range<BigInteger> getTokenRange(TaskContext taskContext)
    {
        return writerContext.job().getTokenPartitioner().getTokenRange(taskContext.partitionId());
    }

    private void writeRow(Tuple2<DecoratedKey, Object[]> keyAndRowData,
                          Map<String, Object> valueMap,
                          int partitionId,
                          Range<BigInteger> range) throws IOException
    {
        DecoratedKey key = keyAndRowData._1();
        BigInteger token = key.getToken();
        Preconditions.checkState(range.contains(token),
                                 String.format("Received Token %s outside of expected range %s", token, range));
        try
        {
            streamSession.addRow(token, getBindValuesForColumns(valueMap, columnNames, keyAndRowData._2()));
        }
        catch (RuntimeException exception)
        {
            String message = String.format("[%s]: Failed to write data to SSTable: SBW DecoratedKey was %s",
                                           partitionId, key);
            LOGGER.error(message, exception);
            throw exception;
        }
    }

    private Map<String, Object> getBindValuesForColumns(Map<String, Object> map, String[] columnNames, Object[] values)
    {
        Preconditions.checkArgument(values.length == columnNames.length,
                                    "Number of values does not match the number of columns " + values.length + ", " + columnNames.length);
        for (int i = 0; i < columnNames.length; i++)
        {
            map.put(columnNames[i], maybeConvertUdt(values[i]));
        }
        return map;
    }

    private Object maybeConvertUdt(Object value)
    {
        if (value instanceof BridgeUdtValue)
        {
            BridgeUdtValue udtValue = (BridgeUdtValue) value;
            // Depth-first replacement of BridgeUdtValue instances to their appropriate Cql types
            for (Map.Entry<String, Object> entry : udtValue.udtMap.entrySet())
            {
                if (entry.getValue() instanceof BridgeUdtValue)
                {
                    udtValue.udtMap.put(entry.getKey(), maybeConvertUdt(entry.getValue()));
                }
            }
            return getUdt(udtValue.name).convertForCqlWriter(udtValue.udtMap, writerContext.bridge().getVersion());
        }
        return value;
    }

    private synchronized CqlField.CqlType getUdt(String udtName)
    {
        return udtCache.computeIfAbsent(udtName, name -> {
            for (CqlField.CqlUdt udt1 : cqlTable().udts())
            {
                if (udt1.cqlName().equals(name))
                {
                    return udt1;
                }
            }
            throw new IllegalArgumentException("Could not find udt with name " + name);
        });
    }

    /**
     * Flushes the written rows and schedule a stream session with the produced sstable asynchronously.
     * Finally, nullify {@link RecordWriter#streamSession}.
     *
     * @param partitionId partition id
     * @throws IOException I/O exceptions during flush
     */
    private void flushAsync(int partitionId) throws IOException
    {
        Preconditions.checkState(streamSession != null);
        LOGGER.info("[{}][{}] Closing writer and scheduling SStable stream with {} rows",
                    partitionId, streamSession.sessionID, streamSession.rowCount());
        Future<StreamResult> future = streamSession.finalizeStreamAsync();
        streamFutures.put(streamSession.sessionID, future);
        streamSession = null;
    }

    /**
     * Functional interface that helps with creating {@link SortedSSTableWriter} instances.
     */
    public interface SSTableWriterFactory
    {
        /**
         * Creates a new instance of the {@link SortedSSTableWriter} with the provided {@code writerContext},
         * {@code outDir}, and {@code digestProvider} parameters.
         *
         * @param writerContext   the context for the bulk writer job
         * @param outDir          an output directory where SSTables components will be written to
         * @param digestAlgorithm a digest provider to calculate digests for every SSTable component
         * @param partitionId     partition id
         * @return a new {@link SortedSSTableWriter}
         */
        SortedSSTableWriter create(BulkWriterContext writerContext,
                                   Path outDir,
                                   DigestAlgorithm digestAlgorithm,
                                   int partitionId);
    }

    // The java version of org.apache.spark.InterruptibleIterator
    // An iterator that wraps around an existing iterator to provide task killing functionality.
    // It works by checking the interrupted flag in TaskContext.
    private static class JavaInterruptibleIterator<T> implements Iterator<T>
    {
        private final TaskContext taskContext;
        private final Iterator<T> delegate;

        JavaInterruptibleIterator(TaskContext taskContext, Iterator<T> delegate)
        {
            this.taskContext = taskContext;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext()
        {
            taskContext.killTaskIfInterrupted();
            return delegate.hasNext();
        }

        @Override
        public T next()
        {
            return delegate.next();
        }
    }
}
