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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.request.data.CreateSliceRequestPayload;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
import org.apache.cassandra.bridge.SSTableDescriptor;
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.spark.bulkwriter.BulkWriteValidator;
import org.apache.cassandra.spark.bulkwriter.BulkWriterContext;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.bulkwriter.SortedSSTableWriter;
import org.apache.cassandra.spark.bulkwriter.StreamError;
import org.apache.cassandra.spark.bulkwriter.StreamResult;
import org.apache.cassandra.spark.bulkwriter.StreamSession;
import org.apache.cassandra.spark.bulkwriter.TransportContext;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedCloudStorageDataTransferApi;
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.common.Digest;
import org.apache.cassandra.spark.common.SSTables;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.exception.ConsistencyNotSatisfiedException;
import org.apache.cassandra.spark.exception.S3ApiCallException;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;

/**
 * {@link StreamSession} implementation that is used for streaming bundled SSTables for S3_COMPAT transport option.
 */
public class CloudStorageStreamSession extends StreamSession<TransportContext.CloudStorageTransportContext>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CloudStorageStreamSession.class);
    private static final String WRITE_PHASE = "UploadAndPrepareToImport";
    protected final BundleNameGenerator bundleNameGenerator;
    protected final CloudStorageDataTransferApi dataTransferApi;
    protected final CassandraBridge bridge;
    private final Set<CreatedRestoreSlice> createdRestoreSlices = new HashSet<>();
    private final SSTablesBundler sstablesBundler;
    private int bundleCount = 0;

    public CloudStorageStreamSession(BulkWriterContext bulkWriterContext, SortedSSTableWriter sstableWriter,
                                     TransportContext.CloudStorageTransportContext transportContext,
                                     String sessionID, Range<BigInteger> tokenRange,
                                     ReplicaAwareFailureHandler<RingInstance> failureHandler,
                                     ExecutorService executorService)
    {
        this(bulkWriterContext, sstableWriter, transportContext, sessionID, tokenRange,
             CassandraBridgeFactory.get(bulkWriterContext.cluster().getLowestCassandraVersion()),
             failureHandler, executorService);
    }

    @VisibleForTesting
    public CloudStorageStreamSession(BulkWriterContext bulkWriterContext, SortedSSTableWriter sstableWriter,
                                     TransportContext.CloudStorageTransportContext transportContext,
                                     String sessionID, Range<BigInteger> tokenRange,
                                     CassandraBridge bridge, ReplicaAwareFailureHandler<RingInstance> failureHandler,
                                     ExecutorService executorService)
    {
        super(bulkWriterContext, sstableWriter, transportContext, sessionID, tokenRange, failureHandler, executorService);

        JobInfo job = bulkWriterContext.job();
        long maxSizePerBundleInBytes = job.transportInfo().getMaxSizePerBundleInBytes();
        this.bundleNameGenerator = new BundleNameGenerator(job.getRestoreJobId().toString(), sessionID);
        this.dataTransferApi = transportContext.dataTransferApi();
        this.bridge = bridge;
        QualifiedTableName qualifiedTableName = job.qualifiedTableName();
        SSTableLister sstableLister = new SSTableLister(qualifiedTableName, bridge);
        Path bundleStagingDir = sstableWriter.getOutDir().resolve("bundle_staging");
        this.sstablesBundler = new SSTablesBundler(bundleStagingDir, sstableLister,
                                                   bundleNameGenerator, maxSizePerBundleInBytes);
    }

    @Override
    protected void onSSTablesProduced(Set<SSTableDescriptor> sstables)
    {
        if (sstables.isEmpty() || isStreamFinalized())
        {
            return;
        }

        executorService.submit(() -> {
            try
            {
                Map<Path, Digest> fileDigests = sstableWriter.prepareSStablesToSend(writerContext, sstables);
                // sstablesBundler keeps track of the known files. No need to record the streamed files.
                // group the files by sstable (unique) basename and add to bundler
                fileDigests.keySet()
                           .stream()
                           .collect(Collectors.groupingBy(SSTables::getSSTableBaseName))
                           .values()
                           .forEach(sstablesBundler::includeSSTable);

                if (!sstablesBundler.hasNext())
                {
                    // hold on until a bundle can be produced
                    return;
                }

                bundleCount += 1;
                Bundle bundle = sstablesBundler.next();
                try
                {
                    sendBundle(bundle, false);
                }
                catch (RuntimeException e)
                {
                    // log and rethrow
                    LOGGER.error("[{}]: Unexpected exception while upload SSTable", sessionID, e);
                    setLastStreamFailure(e);
                }
                finally
                {
                    bundle.deleteAll();
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    protected StreamResult doFinalizeStream()
    {
        sstablesBundler.includeDirectory(sstableWriter.getOutDir());

        sstablesBundler.finish();

        // last stream produces no bundle, and there is no bundle before that (as tracked by bundleCount)
        if (!sstablesBundler.hasNext() && bundleCount == 0)
        {
            if (sstableWriter.sstableCount() != 0)
            {
                LOGGER.error("[{}] SSTable writer has produced files, but no bundle is produced", sessionID);
                throw new RuntimeException("Bundle expected but not found");
            }

            LOGGER.warn("[{}] SSTableBundler does not produce any bundle to send", sessionID);
            return CloudStorageStreamResult.empty(sessionID, tokenRange);
        }

        sendRemainingSSTables();
        LOGGER.info("[{}]: Uploaded bundles to S3. sstables={} bundles={}", sessionID, sstableWriter.sstableCount(), bundleCount);

        int sliceCount = writerContext.job().isCoordinatedWriteEnabled() ? bundleCount : createdRestoreSlices.size();
        CloudStorageStreamResult streamResult = new CloudStorageStreamResult(sessionID,
                                                                             tokenRange,
                                                                             errors,
                                                                             replicas,
                                                                             createdRestoreSlices,
                                                                             sliceCount,
                                                                             sstableWriter.rowCount(),
                                                                             sstableWriter.bytesWritten());
        LOGGER.info("StreamResult: {}", streamResult);
        // CL validation is only required for non-coordinated write.
        // For coordinated write, executors only need to make sure the bundles are uploaded and restore slice is created on sidecar.
        if (!writerContext.job().isCoordinatedWriteEnabled())
        {
            // If the number of successful createSliceRequests cannot satisfy the configured consistency level,
            // an exception is thrown and the task is failed. Spark might retry the task.
            BulkWriteValidator.validateClOrFail(tokenRangeMapping, failureHandler, LOGGER, WRITE_PHASE, writerContext.job(), writerContext.cluster());
        }
        return streamResult;
    }

    @Override
    protected void sendRemainingSSTables()
    {
        while (sstablesBundler.hasNext())
        {
            bundleCount++;
            try
            {
                sendBundle(sstablesBundler.next(), false);
            }
            catch (RuntimeException e)
            {
                // log and rethrow
                LOGGER.error("[{}]: Unexpected exception while upload SSTable", sessionID, e);
                throw e;
            }
            finally
            {
                sstablesBundler.cleanupBundle(sessionID);
            }
        }
    }

    void sendBundle(Bundle bundle, boolean hasRefreshedCredentials)
    {
        StorageCredentials writeCredentials = getStorageCredentialsFromSidecar();
        BundleStorageObject bundleStorageObject;
        try
        {
            bundleStorageObject = uploadBundle(writeCredentials, bundle);
        }
        catch (Exception e)
        {
            // the credential might have expired; retry once
            if (!hasRefreshedCredentials)
            {
                sendBundle(bundle, true);
                return;
            }
            else
            {
                LOGGER.error("[{}]: Failed to send SSTables after refreshing token", sessionID, e);
                throw new RuntimeException(e);
            }
        }
        createRestoreSliceOnSidecar(bundleStorageObject);
    }

    private void createRestoreSliceOnSidecar(BundleStorageObject bundleStorageObject)
    {
        if (writerContext.job().isCoordinatedWriteEnabled())
        {
            CoordinatedCloudStorageDataTransferApi coordinatedApi = (CoordinatedCloudStorageDataTransferApi) dataTransferApi;
            coordinatedApi.forEach((clusterId, ignored) -> {
                String readBucket = transportContext.transportConfiguration().readAccessConfiguration(clusterId).bucket();
                CreateSliceRequestPayload slicePayload = toCreateSliceRequestPayload(bundleStorageObject, readBucket);
                coordinatedApi.createRestoreSliceFromExecutor(clusterId, slicePayload);
            });
            // no need to add to the createdRestoreSlices. Driver polls the progress of the entire restore job.
        }
        else
        {
            CreateSliceRequestPayload slicePayload = toCreateSliceRequestPayload(bundleStorageObject);
            // Create slices on all replicas; remove the _failed_ replica if operation fails
            replicas.removeIf(replica -> !tryCreateRestoreSlicePerReplica(replica, slicePayload));
            // exit early if all replicas has failed, i.e. CL cannot be satisfied for sure.
            if (replicas.isEmpty())
            {
                throw new ConsistencyNotSatisfiedException("All replicas of the token range has failed. tokenRange: " + getTokenRange());
            }
            createdRestoreSlices.add(new CreatedRestoreSlice(slicePayload));
        }
    }

    private StorageCredentials getStorageCredentialsFromSidecar()
    {
        try
        {
            RestoreJobSummaryResponsePayload summary = dataTransferApi.restoreJobSummary();
            return StorageCredentials.fromSidecarCredentials(summary.secrets().writeCredentials());
        }
        catch (SidecarApiCallException exception)
        {
            LOGGER.error("[{}]: Failed to get restore job summary during uploading SSTable bundles", sessionID, exception);
            throw exception;
        }
    }

    /**
     * Uploads generated SSTable bundle
     */
    private BundleStorageObject uploadBundle(StorageCredentials writeCredentials, Bundle bundle) throws S3ApiCallException
    {
        BundleStorageObject object = dataTransferApi.uploadBundle(writeCredentials, bundle);
        transportContext.transportExtensionImplementation()
                        .onObjectPersisted(transportContext.transportConfiguration()
                                                           .writeAccessConfiguration()
                                                           .bucket(),
                                           object.storageObjectKey,
                                           bundle.bundleCompressedSize);
        LOGGER.info("[{}]: Uploaded bundle. storageKey={} uncompressedSize={} compressedSize={}",
                    sessionID,
                    object.storageObjectKey,
                    bundle.bundleUncompressedSize,
                    bundle.bundleCompressedSize);
        return object;
    }

    private boolean tryCreateRestoreSlicePerReplica(RingInstance replica, CreateSliceRequestPayload slicePayload)
    {
        try
        {
            SidecarInstance sidecarInstance = Sidecar.toSidecarInstance(replica, writerContext.job().effectiveSidecarPort());
            dataTransferApi.createRestoreSliceFromExecutor(sidecarInstance, slicePayload);
            return true;
        }
        catch (Exception exception)
        {
            LOGGER.error("[{}]: Failed to create slice. instance={}, slicePayload={}",
                         sessionID, replica.nodeName(), slicePayload, exception);
            writerContext.cluster().refreshClusterInfo();
            // the failed range is a sub-range of the tokenRange; it is guaranteed to not wrap-around
            Range<BigInteger> failedRange = Range.openClosed(slicePayload.startToken(), slicePayload.endToken());
            this.failureHandler.addFailure(failedRange, replica, exception.getMessage());
            errors.add(new StreamError(failedRange, replica, exception.getMessage()));
            // Do not abort job on a single slice failure
            // per Doug: Thinking about this more, you probably shouldn't abort the job in any class that's running in
            // the executors - just bubble up the exception, let Spark retry the task, and if it fails enough times catch the issue
            // in the driver and abort the job there. Aborting the job here and then throwing will cause Spark to retry
            // the task but since you've already aborted the job there's no retry that's really possible. Would it be
            // possible to track the task/slice ID and just abort processing that slice here? Essentially,
            // try to get the Sidecar to skip any uploaded data on an aborted task but let Spark's retry mechanism continue to work properly.
            //
            // Re: the question of just abort processing the slice. If the createSliceRequest fails,
            // the sidecar instance does not import the slice, since there is no such task.
            // Unlike the DIRECT mode, the files is uploaded to blob and there is no file on the disk of the sidecar instance
            // Therefore, no need to clean up the failed files on the failed instance.
            // However, the minority, which has created slices successfully, continue to process and import the slice.
            // The slice data present in the minority of the replica set.
            return false;
        }
    }

    private CreateSliceRequestPayload toCreateSliceRequestPayload(BundleStorageObject bundleStorageObject)
    {
        return toCreateSliceRequestPayload(bundleStorageObject,
                                           transportContext.transportConfiguration().readAccessConfiguration(null).bucket());
    }

    private CreateSliceRequestPayload toCreateSliceRequestPayload(BundleStorageObject bundleStorageObject, String readBucket)
    {
        Bundle bundle = bundleStorageObject.bundle;
        String sliceId = generateSliceId(bundle.bundleSequence);
        return new CreateSliceRequestPayload(sliceId,
                                             0, // todo: Yifan, assign meaningful bucket id
                                             readBucket,
                                             bundleStorageObject.storageObjectKey,
                                             bundleStorageObject.storageObjectChecksum,
                                             bundle.startToken,
                                             bundle.endToken,
                                             bundle.bundleUncompressedSize,
                                             bundle.bundleCompressedSize);
    }

    private String generateSliceId(int bundleSequence)
    {
        return sessionID + '-' + bundleSequence;
    }

    @VisibleForTesting
    Set<CreatedRestoreSlice> createdRestoreSlices()
    {
        return createdRestoreSlices;
    }
}
