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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.bridge.SSTableDescriptor;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.util.IntWrapper;

public class DirectStreamSession extends StreamSession<TransportContext.DirectDataBulkWriterContext>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectStreamSession.class);
    private static final String WRITE_PHASE = "UploadAndCommit";
    private final AtomicInteger nextSSTableIdx = new AtomicInteger(1);
    private final DirectDataTransferApi directDataTransferApi;

    public DirectStreamSession(BulkWriterContext writerContext,
                               SortedSSTableWriter sstableWriter,
                               TransportContext.DirectDataBulkWriterContext transportContext,
                               String sessionID,
                               Range<BigInteger> tokenRange,
                               ReplicaAwareFailureHandler<RingInstance> failureHandler,
                               ExecutorService executorService)
    {
        super(writerContext, sstableWriter, transportContext, sessionID, tokenRange, failureHandler, executorService);
        this.directDataTransferApi = transportContext.dataTransferApi();
    }

    @Override
    protected void onSSTablesProduced(Set<SSTableDescriptor> sstables)
    {
        // do not submit the streaming task if it is in the last stream run, the rest of the sstables should be handled by finalizeStreamAsync
        if (sstables.isEmpty() || isStreamFinalized())
        {
            return;
        }

        // Send sstables asynchronously.
        // SAFETY: sstableWriter.prepareSStablesToSend() is synchronized and can be called
        // concurrently with close() from the RecordWriter thread.
        executorService.submit(() -> {
            try
            {
                // The task does those steps
                // 1. find the newly produced sstables
                // 2. validate the sstables
                // 3. send the sstables to all replicas
                // 4. remove the sstables once sent
                SortedSSTableWriter.PreparedSSTables preparedSSTables = sstableWriter.prepareSStablesToSend(writerContext, sstables);
                IntWrapper sstableCounter = new IntWrapper();
                preparedSSTables.sstables()
                                .forEach(preparedSSTable -> {
                                    sstableCounter.value++;
                                    sendSStableToReplicas(preparedSSTable);
                                });

                LOGGER.info("[{}]: Sent newly produced SSTables. sstables={}", sessionID, sstableCounter.value);
                Set<Path> allSSTableFiles = preparedSSTables.files();
                LOGGER.info("[{}]: Removing temporary files after streaming. files={}", sessionID, allSSTableFiles);
                allSSTableFiles.forEach(path -> {
                    try
                    {
                        Files.deleteIfExists(path);
                    }
                    catch (IOException e)
                    {
                        LOGGER.warn("[{}]: Failed to delete temporary file. file={}", sessionID, path);
                    }
                });
            }
            catch (IOException e)
            {
                LOGGER.error("[{}]: Unexpected exception while streaming SSTables {}",
                             sessionID, sstableWriter.getOutDir());
                setLastStreamFailure(e);
                cleanAllReplicas();
            }
        });
    }

    @Override
    protected StreamResult doFinalizeStream()
    {
        sendRemainingSSTables();
        // StreamResult has errors streaming to replicas
        DirectStreamResult streamResult = new DirectStreamResult(sessionID,
                                                                 tokenRange,
                                                                 errors,
                                                                 new ArrayList<>(replicas),
                                                                 sstableWriter.rowCount(),
                                                                 sstableWriter.bytesWritten());
        List<CommitResult> cr;
        try
        {
            cr = commit(streamResult);
        }
        catch (Exception e)
        {
            if (e instanceof InterruptedException)
            {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(e);
        }
        streamResult.setCommitResults(cr);
        LOGGER.debug("StreamResult: {}", streamResult);
        // Check consistency given the no. failures
        BulkWriteValidator.validateClOrFail(tokenRangeMapping, failureHandler, LOGGER, WRITE_PHASE, writerContext.job(), writerContext.cluster());
        return streamResult;
    }

    @Override
    protected void sendRemainingSSTables()
    {
        try
        {
            sstableWriter.remainingSSTablesAfterClose()
                         .sstables()
                         .forEach(this::sendSStableToReplicas);

            LOGGER.info("[{}]: Sent SSTables. sstables={}", sessionID, sstableWriter.sstableCount());
        }
        catch (Exception exception)
        {
            LOGGER.error("[{}]: Unexpected exception while streaming SSTables {}",
                         sessionID, sstableWriter.getOutDir());
            cleanAllReplicas();
            throw new RuntimeException(exception);
        }
        finally
        {
            // Clean up SSTable files once the task is complete
            cleanupSSTables(LOGGER);
        }
    }

    private void sendSStableToReplicas(SortedSSTableWriter.PreparedSSTable preparedSSTable)
    {
        int ssTableIdx = nextSSTableIdx.getAndIncrement();

        LOGGER.info("[{}]: Pushing SSTable {} to replicas {}",
                    sessionID, preparedSSTable.dataFile(),
                    replicas.stream().map(RingInstance::nodeName).collect(Collectors.joining(",")));
        replicas.removeIf(replica -> !trySendSSTableToOneReplica(preparedSSTable, ssTableIdx, replica));
    }

    private boolean trySendSSTableToOneReplica(SortedSSTableWriter.PreparedSSTable preparedSSTable,
                                               int ssTableIdx,
                                               RingInstance replica)
    {
        try
        {
            sendSSTableToOneReplica(preparedSSTable, ssTableIdx, replica);
            return true;
        }
        catch (Exception exception)
        {
            LOGGER.error("[{}]: Failed to stream range {} to instance {}",
                         sessionID, tokenRange, replica.nodeName(), exception);
            writerContext.cluster().refreshClusterInfo();
            // Sometimes error can contain just file name (e.g. when it is missing).
            // Let us return 3 latest stacktrace lines for easier troubleshooting.
            String stackTrace = Arrays.stream(exception.getStackTrace())
                                      .limit(3)
                                      .map(StackTraceElement::toString)
                                      .collect(Collectors.joining("\n"));
            String errorMessage = exception.getClass().getName() + ": " + exception.getMessage()
                                  + "\n" + stackTrace;
            failureHandler.addFailure(this.tokenRange, replica, errorMessage);
            errors.add(new StreamError(this.tokenRange, replica, errorMessage));
            clean(replica, sessionID);
            return false;
        }
    }

    private void sendSSTableToOneReplica(SortedSSTableWriter.PreparedSSTable preparedSSTable,
                                         int ssTableIdx,
                                         RingInstance instance) throws IOException
    {
        for (Path componentFile : preparedSSTable.files())
        {
            // send data component the last
            if (componentFile.equals(preparedSSTable.dataFile()))
            {
                continue;
            }
            sendSSTableComponent(componentFile, ssTableIdx, instance, preparedSSTable.getDigest(componentFile));
        }
        Preconditions.checkNotNull(preparedSSTable.dataFile(), "Data file not present in SSTable: {}", preparedSSTable);
        sendSSTableComponent(preparedSSTable.dataFile(), ssTableIdx, instance, preparedSSTable.getDigest(preparedSSTable.dataFile()));
    }

    private void sendSSTableComponent(Path componentFile,
                                      int ssTableIdx,
                                      RingInstance instance,
                                      Digest digest) throws IOException
    {
        Preconditions.checkNotNull(digest, "All files must have a digest. SSTableWriter should have calculated these.");
        LOGGER.info("[{}]: Uploading {} to {}: size={} digest={}",
                    sessionID, componentFile, instance.nodeName(), Files.size(componentFile), digest);
        directDataTransferApi.uploadSSTableComponent(componentFile, ssTableIdx, instance, this.sessionID, digest);
    }

    private List<CommitResult> commit(DirectStreamResult streamResult) throws ExecutionException, InterruptedException
    {
        try (CommitCoordinator cc = CommitCoordinator.commit(writerContext, transportContext, streamResult))
        {
            List<CommitResult> commitResults = cc.get();
            LOGGER.debug("All CommitResults: {}", commitResults);
            commitResults.forEach(cr -> BulkWriteValidator.updateFailureHandler(cr, WRITE_PHASE, failureHandler));
            return commitResults;
        }
    }

    /* Get all replicas and clean temporary state on them */
    private void cleanAllReplicas()
    {
        Set<RingInstance> instances = new HashSet<>(replicas);
        errors.forEach(streamError -> instances.add(streamError.instance));
        instances.forEach(instance -> clean(instance, sessionID));
    }

    private void clean(RingInstance instance, String sessionID)
    {
        if (writerContext.job().getSkipClean())
        {
            LOGGER.info("Skip clean requested - not cleaning SSTable session {} on instance {}",
                        sessionID, instance.nodeName());
            return;
        }
        String jobID = writerContext.job().getId();
        LOGGER.info("Cleaning SSTable session {} on instance {}", sessionID, instance.nodeName());
        try
        {
            directDataTransferApi.cleanUploadSession(instance, sessionID, jobID);
        }
        catch (Exception exception)
        {
            LOGGER.warn("Failed to clean SSTables on {} for session {} and ignoring errMsg",
                        instance.nodeName(), sessionID, exception);
        }
    }
}
