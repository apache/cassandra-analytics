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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.token.CassandraRing;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.common.MD5Hash;
import org.apache.cassandra.spark.common.SSTables;

public class StreamSession
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamSession.class);
    private final BulkWriterContext writerContext;
    private final String sessionID;
    private final Range<BigInteger> tokenRange;
    final List<RingInstance> replicas;
    private final ArrayList<StreamError> errors = new ArrayList<>();
    private final ReplicaAwareFailureHandler<RingInstance> failureHandler;
    private final AtomicInteger nextSSTableIdx = new AtomicInteger(1);
    private final ExecutorService executor;
    private final List<Future<?>> futures = new ArrayList<>();
    private final CassandraRing<RingInstance> ring;
    private static final String WRITE_PHASE = "UploadAndCommit";

    public StreamSession(BulkWriterContext writerContext, String sessionID, Range<BigInteger> tokenRange)
    {
        this(writerContext, sessionID, tokenRange, Executors.newSingleThreadExecutor());
    }

    @VisibleForTesting
    public StreamSession(BulkWriterContext writerContext,
                         String sessionID,
                         Range<BigInteger> tokenRange,
                         ExecutorService executor)
    {
        this.writerContext = writerContext;
        this.ring = writerContext.cluster().getRing(true);
        this.failureHandler = new ReplicaAwareFailureHandler<>(ring);
        this.sessionID = sessionID;
        this.tokenRange = tokenRange;
        this.replicas = getReplicas();
        this.executor = executor;
    }

    public void scheduleStream(SSTableWriter ssTableWriter)
    {
        Preconditions.checkState(!ssTableWriter.getTokenRange().isEmpty(), "Trying to stream empty SSTable");

        Preconditions.checkState(tokenRange.encloses(ssTableWriter.getTokenRange()),
                                 String.format("SSTable range %s should be enclosed in the partition range %s",
                                               ssTableWriter.getTokenRange(), tokenRange));

        futures.add(executor.submit(() -> sendSSTables(writerContext, ssTableWriter)));
    }

    public StreamResult close() throws ExecutionException, InterruptedException
    {
        for (Future future : futures)
        {
            try
            {
                future.get();
            }
            catch (Exception exception)
            {
                LOGGER.error("Unexpected stream errMsg. "
                           + "Stream errors should have converted to StreamError and sent to driver", exception);
                throw new RuntimeException(exception);
            }
        }

        executor.shutdown();
        LOGGER.info("[{}]: Closing stream session. Sent {} SSTables", sessionID, futures.size());

        if (futures.isEmpty())
        {
            return new StreamResult(sessionID, tokenRange, new ArrayList<>(), new ArrayList<>());
        }
        else
        {
            StreamResult streamResult = new StreamResult(sessionID, tokenRange, errors, new ArrayList<>(replicas));
            List<CommitResult> cr = commit(streamResult);
            streamResult.setCommitResults(cr);
            LOGGER.debug("StreamResult: {}", streamResult);
            BulkWriteValidator.validateClOrFail(failureHandler, LOGGER, WRITE_PHASE, writerContext.job());
            return streamResult;
        }
    }

    private List<CommitResult> commit(StreamResult streamResult) throws ExecutionException, InterruptedException
    {
        try (CommitCoordinator cc = CommitCoordinator.commit(writerContext, new StreamResult[]{streamResult}))
        {
            List<CommitResult> commitResults = cc.get();
            LOGGER.debug("All CommitResults: {}", commitResults);
            commitResults.forEach(cr -> BulkWriteValidator.updateFailureHandler(cr, WRITE_PHASE, failureHandler));
            return commitResults;
        }
    }

    @VisibleForTesting
    List<RingInstance> getReplicas()
    {
        Map<Range<BigInteger>, List<RingInstance>> overlappingRanges = ring.getSubRanges(tokenRange).asMapOfRanges();

        Preconditions.checkState(overlappingRanges.keySet().size() == 1,
                                 String.format("Partition range %s is mapping more than one range %s",
                                               tokenRange, overlappingRanges));

        List<RingInstance> replicaList = overlappingRanges.values().stream()
                                                          .flatMap(Collection::stream)
                                                          .distinct()
                                                          .collect(Collectors.toList());
        List<RingInstance> availableReplicas = validateReplicas(replicaList);
        // In order to better utilize replicas, shuffle the replicaList so each session starts writing to a different replica first
        Collections.shuffle(availableReplicas);
        return availableReplicas;
    }

    private List<RingInstance> validateReplicas(List<RingInstance> replicaList)
    {
        Map<Boolean, List<RingInstance>> groups = replicaList.stream()
                .collect(Collectors.partitioningBy(writerContext.cluster()::instanceIsAvailable));
        groups.get(false).forEach(instance -> {
            String errorMessage = String.format("Instance %s is not available.", instance.getNodeName());
            failureHandler.addFailure(tokenRange, instance, errorMessage);
            errors.add(new StreamError(instance, errorMessage));
        });
        return groups.get(true);
    }

    private void sendSSTables(BulkWriterContext writerContext, SSTableWriter ssTableWriter)
    {
        try (DirectoryStream<Path> dataFileStream = Files.newDirectoryStream(ssTableWriter.getOutDir(), "*Data.db"))
        {
            for (Path dataFile : dataFileStream)
            {
                int ssTableIdx = nextSSTableIdx.getAndIncrement();

                LOGGER.info("[{}]: Pushing SSTable {} to replicas {}",
                            sessionID, dataFile, replicas.stream()
                                                         .map(RingInstance::getNodeName)
                                                         .collect(Collectors.joining(",")));
                replicas.removeIf(replica -> !trySendSSTableToReplica(writerContext, ssTableWriter, dataFile, ssTableIdx, replica));
            }
        }
        catch (IOException exception)
        {
            LOGGER.error("[{}]: Unexpected exception while streaming SSTables {}",
                         sessionID, ssTableWriter.getOutDir());
            cleanAllReplicas();
            throw new RuntimeException(exception);
        }
        finally
        {
            // Clean up SSTable files once the task is complete
            File tempDir = ssTableWriter.getOutDir().toFile();
            LOGGER.info("[{}]:Removing temporary files after stream session from {}", sessionID, tempDir);
            try
            {
                FileUtils.deleteDirectory(tempDir);
            }
            catch (IOException exception)
            {
                LOGGER.warn("[{}]:Failed to delete temporary directory {}", sessionID, tempDir, exception);
            }
        }
    }

    private boolean trySendSSTableToReplica(BulkWriterContext writerContext,
                                            SSTableWriter ssTableWriter,
                                            Path dataFile,
                                            int ssTableIdx,
                                            RingInstance replica)
    {
        try
        {
            sendSSTableToReplica(writerContext, dataFile, ssTableIdx, replica, ssTableWriter.getFileHashes());
            return true;
        }
        catch (Exception exception)
        {
            LOGGER.error("[{}]: Failed to stream range {} to instance {}",
                         sessionID, tokenRange, replica.getNodeName(), exception);
            writerContext.cluster().refreshClusterInfo();
            failureHandler.addFailure(tokenRange, replica, exception.getMessage());
            errors.add(new StreamError(replica, exception.getMessage()));
            clean(writerContext, replica, sessionID);
            return false;
        }
    }

    /**
     * Get all replicas and clean temporary state on them
     */
    private void cleanAllReplicas()
    {
        Set<RingInstance> instances = new HashSet<>(replicas);
        errors.forEach(streamError -> instances.add(streamError.instance));
        instances.forEach(instance -> clean(writerContext, instance, sessionID));
    }

    private void sendSSTableToReplica(BulkWriterContext writerContext,
                                      Path dataFile,
                                      int ssTableIdx,
                                      RingInstance instance,
                                      Map<Path, MD5Hash> fileHashes) throws Exception
    {
        try (DirectoryStream<Path> componentFileStream =
                Files.newDirectoryStream(dataFile.getParent(), SSTables.getSSTableBaseName(dataFile) + "*"))
        {
            for (Path componentFile : componentFileStream)
            {
                if (componentFile.getFileName().toString().endsWith("Data.db"))
                {
                    continue;
                }
                sendSSTableComponent(writerContext, componentFile, ssTableIdx, instance, fileHashes.get(componentFile));
            }
            sendSSTableComponent(writerContext, dataFile, ssTableIdx, instance, fileHashes.get(dataFile));
        }
    }

    private void sendSSTableComponent(BulkWriterContext writerContext,
                                      Path componentFile,
                                      int ssTableIdx,
                                      RingInstance instance,
                                      MD5Hash fileHash) throws Exception
    {
        Preconditions.checkNotNull(fileHash, "All files must have a hash. SSTableWriter should have calculated these. This is a bug.");
        long fileSize = Files.size(componentFile);
        LOGGER.info("[{}]: Uploading {} to {}: Size is {}", sessionID, componentFile, instance.getNodeName(), fileSize);
        writerContext.transfer().uploadSSTableComponent(componentFile, ssTableIdx, instance, sessionID, fileHash);
    }

    public static void clean(BulkWriterContext writerContext, RingInstance instance, String sessionID)
    {
        if (writerContext.job().getSkipClean())
        {
            LOGGER.info("Skip clean requested - not cleaning SSTable session {} on instance {}",
                        sessionID, instance.getNodeName());
            return;
        }
        String jobID = writerContext.job().getId().toString();
        LOGGER.info("Cleaning SSTable session {} on instance {}", sessionID, instance.getNodeName());
        try
        {
            writerContext.transfer().cleanUploadSession(instance, sessionID, jobID);
        }
        catch (Exception exception)
        {
            LOGGER.warn("Failed to clean SSTables on {} for session {} and ignoring errMsg",
                        instance.getNodeName(), sessionID, exception);
        }
    }
}
