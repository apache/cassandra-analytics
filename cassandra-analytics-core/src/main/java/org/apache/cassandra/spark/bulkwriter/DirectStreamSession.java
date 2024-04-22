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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.common.SSTables;

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
                               ReplicaAwareFailureHandler<RingInstance> failureHandler)
    {
        super(writerContext, sstableWriter, transportContext, sessionID, tokenRange, failureHandler);
        this.directDataTransferApi = transportContext.dataTransferApi();
    }

    @Override
    protected StreamResult doScheduleStream(SortedSSTableWriter sstableWriter)
    {
        sendSSTables(sstableWriter);
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
        BulkWriteValidator.validateClOrFail(tokenRangeMapping, failureHandler, LOGGER, WRITE_PHASE, writerContext.job());
        return streamResult;
    }

    @Override
    protected void sendSSTables(final SortedSSTableWriter sstableWriter)
    {
        try (DirectoryStream<Path> dataFileStream = Files.newDirectoryStream(sstableWriter.getOutDir(), "*Data.db"))
        {
            for (Path dataFile : dataFileStream)
            {
                int ssTableIdx = nextSSTableIdx.getAndIncrement();

                LOGGER.info("[{}]: Pushing SSTable {} to replicas {}",
                            sessionID, dataFile,
                            replicas.stream().map(RingInstance::nodeName).collect(Collectors.joining(",")));
                replicas.removeIf(replica -> !trySendSSTableToReplica(sstableWriter, dataFile, ssTableIdx, replica));
            }

            LOGGER.info("[{}]: Sent SSTables. sstables={}", sessionID, sstableWriter.sstableCount());
        }
        catch (IOException exception)
        {
            LOGGER.error("[{}]: Unexpected exception while streaming SSTables {}",
                         sessionID, sstableWriter.getOutDir());
            cleanAllReplicas();
            throw new RuntimeException(exception);
        }
        finally
        {
            // Clean up SSTable files once the task is complete
            File tempDir = sstableWriter.getOutDir().toFile();
            LOGGER.info("[{}]: Removing temporary files after stream session from {}", sessionID, tempDir);
            try
            {
                FileUtils.deleteDirectory(tempDir);
            }
            catch (IOException exception)
            {
                LOGGER.warn("[{}]: Failed to delete temporary directory {}", sessionID, tempDir, exception);
            }
        }
    }

    private boolean trySendSSTableToReplica(SortedSSTableWriter sstableWriter,
                                            Path dataFile,
                                            int ssTableIdx,
                                            RingInstance replica)
    {
        try
        {
            sendSSTableToReplica(dataFile, ssTableIdx, replica, sstableWriter.fileDigestMap());
            return true;
        }
        catch (Exception exception)
        {
            LOGGER.error("[{}]: Failed to stream range {} to instance {}",
                         sessionID, tokenRange, replica.nodeName(), exception);
            writerContext.cluster().refreshClusterInfo();
            failureHandler.addFailure(this.tokenRange, replica, exception.getMessage());
            errors.add(new StreamError(this.tokenRange, replica, exception.getMessage()));
            clean(replica, sessionID);
            return false;
        }
    }

    private void sendSSTableToReplica(Path dataFile,
                                      int ssTableIdx,
                                      RingInstance instance,
                                      Map<Path, Digest> fileHashes) throws IOException
    {
        try (DirectoryStream<Path> componentFileStream = Files.newDirectoryStream(dataFile.getParent(),
                                                                                  SSTables.getSSTableBaseName(dataFile) + "*"))
        {
            for (Path componentFile : componentFileStream)
            {
                // send data component the last
                if (componentFile.getFileName().toString().endsWith("Data.db"))
                {
                    continue;
                }
                sendSSTableComponent(componentFile, ssTableIdx, instance, fileHashes.get(componentFile));
            }
            sendSSTableComponent(dataFile, ssTableIdx, instance, fileHashes.get(dataFile));
        }
    }

    private void sendSSTableComponent(Path componentFile,
                                      int ssTableIdx,
                                      RingInstance instance,
                                      Digest digest) throws IOException
    {
        Preconditions.checkNotNull(digest, "All files must have a digest. SSTableWriter should have calculated these.");
        LOGGER.info("[{}]: Uploading {} to {}: Size is {}",
                    sessionID, componentFile, instance.nodeName(), Files.size(componentFile));
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
