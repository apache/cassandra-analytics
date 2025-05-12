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

package org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.data.RestoreJobProgressFetchPolicy;
import o.a.c.sidecar.client.shaded.common.request.CreateRestoreJobSliceRequest;
import o.a.c.sidecar.client.shaded.common.request.data.CreateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.request.data.CreateSliceRequestPayload;
import o.a.c.sidecar.client.shaded.common.request.data.RestoreJobProgressRequestParams;
import o.a.c.sidecar.client.shaded.common.request.data.UpdateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobProgressResponsePayload;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.sidecar.client.RequestContext;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.Bundle;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.BundleStorageObject;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.CloudStorageDataTransferApi;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.CloudStorageDataTransferApiImpl;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.exception.S3ApiCallException;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CoordinatedCloudStorageDataTransferApi implements CloudStorageDataTransferApi,
                                                               CoordinatedCloudStorageDataTransferApiExtension,
                                                               MultiClusterSupport<CloudStorageDataTransferApiImpl>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatedCloudStorageDataTransferApi.class);

    private final MultiClusterContainer<CloudStorageDataTransferApiImpl> dataTransferApis = new MultiClusterContainer<>();
    private final RateLimiter sidecarApiCallRateLimiter;

    public CoordinatedCloudStorageDataTransferApi(@NotNull Map<String, CloudStorageDataTransferApiImpl> dataTransferApiByCluster)
    {
        // todo: expose rate limiter as configurable when needed
        this(RateLimiter.create(1), dataTransferApiByCluster);
    }

    @VisibleForTesting
    public CoordinatedCloudStorageDataTransferApi(RateLimiter sidecarApiCallRateLimiter,
                                                  @NotNull Map<String, CloudStorageDataTransferApiImpl> dataTransferApiByCluster)
    {
        Preconditions.checkArgument(!dataTransferApiByCluster.isEmpty(), "dataTransferApiByCluster cannot be empty");
        this.dataTransferApis.addAll(dataTransferApiByCluster);
        this.sidecarApiCallRateLimiter = sidecarApiCallRateLimiter;
    }

    @Override
    public int size()
    {
        return dataTransferApis.size();
    }

    @Override
    public void forEach(BiConsumer<String, CloudStorageDataTransferApiImpl> action)
    {
        // do not skip any cluster
        Predicate<String> allowAllClusters = id -> false;
        forEachInternal(allowAllClusters, action);
    }

    @Override
    public BundleStorageObject uploadBundle(StorageCredentials writeCredentials, Bundle bundle) throws S3ApiCallException
    {
        return dataTransferApis.getAnyValueOrThrow().uploadBundle(writeCredentials, bundle);
    }

    @Override
    public RestoreJobSummaryResponsePayload restoreJobSummary() throws SidecarApiCallException
    {
        return dataTransferApis.getAnyValueOrThrow().restoreJobSummary();
    }

    @Override
    public void updateRestoreJob(UpdateRestoreJobRequestPayload updateRestoreJobRequestPayload) throws SidecarApiCallException
    {
        forEachInternal(api -> api.updateRestoreJob(updateRestoreJobRequestPayload));
    }

    @Override
    public void abortRestoreJob() throws SidecarApiCallException
    {
        forEachInternal(CloudStorageDataTransferApiImpl::abortRestoreJob);
    }

    @Override
    public void createRestoreSliceFromExecutor(String clusterId, CreateSliceRequestPayload createSliceRequestPayload) throws SidecarApiCallException
    {
        createRestoreSliceInternal(dataTransferApis.getValueOrThrow(clusterId), createSliceRequestPayload);
    }

    @Override
    public void restoreJobProgress(RestoreJobProgressFetchPolicy fetchPolicy,
                                   Predicate<String> shouldSkipCluster,
                                   BiConsumer<String, RestoreJobProgressResponsePayload> progressHandler) throws SidecarApiCallException
    {
        forEachInternal(shouldSkipCluster, (clusterId, dataTransferApi) -> {
            restoreJobProgressInternal(clusterId, dataTransferApi, fetchPolicy, progressHandler);
        });
    }

    @Nullable
    @Override
    public CloudStorageDataTransferApiImpl getValueOrNull(@NotNull String clusterId)
    {
        return dataTransferApis.getValueOrNull(clusterId);
    }

    @Override
    public void createRestoreJob(CreateRestoreJobRequestPayload createRestoreJobRequestPayload) throws SidecarApiCallException
    {
        throw new UnsupportedOperationException("Not supported for coordinated write");
    }

    @Override
    public void createRestoreSliceFromExecutor(SidecarInstance sidecarInstance,
                                               CreateSliceRequestPayload createSliceRequestPayload) throws SidecarApiCallException
    {
        throw new UnsupportedOperationException("Not supported for coordinated write");
    }

    @Override
    public CompletableFuture<Void> createRestoreSliceFromDriver(SidecarInstance sidecarInstance,
                                                                CreateSliceRequestPayload createSliceRequestPayload)
    {
        throw new UnsupportedOperationException("Not supported for coordinated write");
    }

    private void createRestoreSliceInternal(CloudStorageDataTransferApiImpl dataTransferApi,
                                            CreateSliceRequestPayload createSliceRequestPayload) throws SidecarApiCallException
    {
        JobInfo jobInfo = dataTransferApi.jobInfo();
        SidecarClient sidecarClient = dataTransferApi.sidecarClient();
        QualifiedTableName qualifiedTableName = jobInfo.qualifiedTableName();
        CreateRestoreJobSliceRequest request = new CreateRestoreJobSliceRequest(qualifiedTableName.keyspace(),
                                                                                qualifiedTableName.table(),
                                                                                jobInfo.getRestoreJobId(),
                                                                                createSliceRequestPayload);
        RetryPolicy retryPolicy = new CloudStorageDataTransferApiImpl.ExecutorCreateSliceRetryPolicy(sidecarClient);
        RequestContext requestContext = sidecarClient.requestBuilder().retryPolicy(retryPolicy).request(request).build();
        try
        {
            sidecarClient.executeRequestAsync(requestContext).get();
        }
        catch (Exception exception)
        {
            handleInterruption(exception);
            throw new SidecarApiCallException("Failed to create restore job slice. " +
                                              "restoreJobId=" + jobInfo.getRestoreJobId() +
                                              " sliceId=" + createSliceRequestPayload.sliceId(),
                                              exception);
        }
    }

    private void restoreJobProgressInternal(String clusterId,
                                            CloudStorageDataTransferApiImpl dataTransferApi,
                                            RestoreJobProgressFetchPolicy fetchPolicy,
                                            BiConsumer<String, RestoreJobProgressResponsePayload> progressHandler)
    {
        JobInfo jobInfo = dataTransferApi.jobInfo();
        QualifiedTableName qualifiedTableName = jobInfo.qualifiedTableName();
        UUID restoreJobId = jobInfo.getRestoreJobId();
        RestoreJobProgressRequestParams requestParams = new RestoreJobProgressRequestParams(qualifiedTableName.keyspace(),
                                                                                            qualifiedTableName.table(),
                                                                                            restoreJobId,
                                                                                            fetchPolicy);
        RestoreJobProgressResponsePayload jobProgress = restoreJobProgress(dataTransferApi, requestParams);
        progressHandler.accept(clusterId, jobProgress);
    }

    private void forEachInternal(Consumer<CloudStorageDataTransferApiImpl> dataTransferApiConsumer)
    {
        forEach((ignored, api) -> dataTransferApiConsumer.accept(api));
    }

    /**
     * Run dataTransferApiAction on the clusters that are not skipped
     * @param shouldSkipCluster if the predicate returns true, skip the cluster; otherwise, run dataTransferApiAction with the cluster
     * @param dataTransferApiAction action that consumes clusterId and data transfer api
     */
    private void forEachInternal(Predicate<String> shouldSkipCluster,
                                 BiConsumer<String, CloudStorageDataTransferApiImpl> dataTransferApiAction)
    {
        dataTransferApis.forEach((clusterId, dataTransferApi) -> {
            if (shouldSkipCluster.test(clusterId))
            {
                LOGGER.debug("Cluster is skipped according to clusterIdFilter. clusterId={}", clusterId);
                return;
            }

            sidecarApiCallRateLimiter.acquire(1);
            try
            {
                dataTransferApiAction.accept(clusterId, dataTransferApi);
            }
            catch (SidecarApiCallException exception)
            {
                LOGGER.error("Fails to call Sidecar. clusterId={}", clusterId, exception);
                throw exception;
            }
        });
    }

    @VisibleForTesting
    RestoreJobProgressResponsePayload restoreJobProgress(CloudStorageDataTransferApiImpl dataTransferApi,
                                                         RestoreJobProgressRequestParams requestParams)
    {
        try
        {
            return dataTransferApi.sidecarClient().restoreJobProgress(requestParams).get();
        }
        catch (Exception exception)
        {
            handleInterruption(exception);
            throw new SidecarApiCallException("Failed to retrieve job progress. restoreJobId=" + requestParams.jobId, exception);
        }
    }
}
