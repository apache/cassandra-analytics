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

package org.apache.cassandra.spark.bulkwriter.blobupload;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.data.CreateSliceRequestPayload;
import o.a.c.sidecar.client.shaded.common.data.QualifiedTableName;
import o.a.c.sidecar.client.shaded.common.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraBridgeFactory;
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
import org.apache.cassandra.spark.bulkwriter.token.ReplicaAwareFailureHandler;
import org.apache.cassandra.spark.common.client.ClientException;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;

/**
 * {@link StreamSession} implementation that is used for streaming bundled SSTables for S3_COMPAT transport option.
 */
public class BlobStreamSession extends StreamSession<TransportContext.CloudStorageTransportContext>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BlobStreamSession.class);
    private static final String WRITE_PHASE = "UploadAndPrepareToImport";
    protected final BundleNameGenerator bundleNameGenerator;
    protected final BlobDataTransferApi blobDataTransferApi;
    protected final CassandraBridge bridge;
    private final Set<CreatedRestoreSlice> createdRestoreSlices = new HashSet<>();
    private final SSTablesBundler sstablesBundler;
    private int bundleCount;

    public BlobStreamSession(BulkWriterContext bulkWriterContext, SortedSSTableWriter sstableWriter,
                             TransportContext.CloudStorageTransportContext transportContext,
                             String sessionID, Range<BigInteger> tokenRange,
                             ReplicaAwareFailureHandler<RingInstance> failureHandler)
    {
        this(bulkWriterContext, sstableWriter, transportContext, sessionID, tokenRange,
             CassandraBridgeFactory.get(bulkWriterContext.cluster().getLowestCassandraVersion()),
             failureHandler);
    }

    @VisibleForTesting
    public BlobStreamSession(BulkWriterContext bulkWriterContext, SortedSSTableWriter sstableWriter,
                             TransportContext.CloudStorageTransportContext transportContext,
                             String sessionID, Range<BigInteger> tokenRange,
                             CassandraBridge bridge, ReplicaAwareFailureHandler<RingInstance> failureHandler)
    {
        super(bulkWriterContext, sstableWriter, transportContext, sessionID, tokenRange, failureHandler);

        JobInfo job = bulkWriterContext.job();
        long maxSizePerBundleInBytes = job.getTransportInfo().getMaxSizePerBundleInBytes();
        this.bundleNameGenerator = new BundleNameGenerator(job.getRestoreJobId().toString(), sessionID);
        this.blobDataTransferApi = transportContext.dataTransferApi();
        this.bridge = bridge;
        QualifiedTableName qualifiedTableName = job.getQualifiedTableName();
        SSTableLister sstableLister = new SSTableLister(qualifiedTableName, bridge);
        Path bundleStagingDir = sstableWriter.getOutDir().resolve("bundle_staging");
        this.sstablesBundler = new SSTablesBundler(bundleStagingDir, sstableLister,
                                                   bundleNameGenerator, maxSizePerBundleInBytes);
    }

    @Override
    protected StreamResult doScheduleStream(SortedSSTableWriter sstableWriter)
    {
        sstablesBundler.includeDirectory(sstableWriter.getOutDir());

        sstablesBundler.finish();

        if (!sstablesBundler.hasNext())
        {
            if (sstableWriter.sstableCount() != 0)
            {
                LOGGER.error("[{}] SSTable writer has produced files, but no bundle is produced", sessionID);
                throw new RuntimeException("Bundle expected but not found");
            }

            LOGGER.warn("[{}] SSTableBundler does not produce any bundle to send", sessionID);
            return BlobStreamResult.empty(sessionID, tokenRange);
        }

        sendSSTables(sstableWriter);
        LOGGER.info("[{}]: Uploaded bundles to S3. sstables={} bundles={}", sessionID, sstableWriter.sstableCount(), bundleCount);

        BlobStreamResult streamResult = new BlobStreamResult(sessionID,
                                                             tokenRange,
                                                             errors,
                                                             replicas,
                                                             createdRestoreSlices,
                                                             sstableWriter.rowCount(),
                                                             sstableWriter.bytesWritten());
        LOGGER.info("StreamResult: {}", streamResult);
        // If the number of successful createSliceRequests cannot satisfy the configured consistency level,
        // an exception is thrown and the task is failed. Spark might retry the task.
        BulkWriteValidator.validateClOrFail(tokenRangeMapping, failureHandler, LOGGER, WRITE_PHASE, writerContext.job());
        return streamResult;
    }

    @Override
    protected void sendSSTables(SortedSSTableWriter sstableWriter)
    {
        bundleCount = 0;
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
        CreateSliceRequestPayload slicePayload = toCreateSliceRequestPayload(bundleStorageObject);
        // Create slices on all replicas; remove the _failed_ replica if operation fails
        replicas.removeIf(replica -> !tryCreateRestoreSlicePerReplica(replica, slicePayload));
        if (!replicas.isEmpty())
        {
            createdRestoreSlices.add(new CreatedRestoreSlice(slicePayload));
        }
    }

    private StorageCredentials getStorageCredentialsFromSidecar()
    {
        RestoreJobSummaryResponsePayload summary;
        try
        {
            summary = blobDataTransferApi.restoreJobSummary();
        }
        catch (ClientException e)
        {
            LOGGER.error("[{}]: Failed to get restore job summary during uploading SSTable bundles", sessionID, e);
            throw new RuntimeException(e);
        }

        return StorageCredentials.fromSidecarCredentials(summary.secrets().writeCredentials());
    }

    /**
     * Uploads generated SSTable bundle
     */
    private BundleStorageObject uploadBundle(StorageCredentials writeCredentials, Bundle bundle) throws ClientException
    {
        BundleStorageObject object = blobDataTransferApi.uploadBundle(writeCredentials, bundle);
        transportContext.transportExtensionImplementation()
                        .onObjectPersisted(transportContext.transportConfiguration()
                                                           .getWriteBucket(),
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
            SidecarInstance sidecarInstance = Sidecar.toSidecarInstance(replica, writerContext.conf());
            blobDataTransferApi.createRestoreSliceFromExecutor(sidecarInstance, slicePayload);
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
        Bundle bundle = bundleStorageObject.bundle;
        String sliceId = generateSliceId(bundle.bundleSequence);
        return new CreateSliceRequestPayload(sliceId,
                                             0, // todo: Yifan, assign meaningful bucket id
                                             transportContext.transportConfiguration()
                                                             .getReadBucket(),
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
