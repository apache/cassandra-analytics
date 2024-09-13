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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpResponseStatus;
import o.a.c.sidecar.client.shaded.common.request.CreateRestoreJobSliceRequest;
import o.a.c.sidecar.client.shaded.common.request.Request;
import o.a.c.sidecar.client.shaded.common.request.data.CreateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.request.data.CreateSliceRequestPayload;
import o.a.c.sidecar.client.shaded.common.request.data.UpdateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.response.data.CreateRestoreJobResponsePayload;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.client.HttpResponseImpl;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.retry.RetryAction;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.common.client.ClientException;
import org.apache.cassandra.spark.data.QualifiedTableName;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;

/**
 * Encapsulates transfer APIs used by {@link BlobStreamSession}. {@link StorageClient} is used to interact with S3 and
 * upload SSTables bundles to S3 bucket. It also has {@link SidecarClient} to call relevant sidecar APIs.
 */
public class BlobDataTransferApi
{
    private final JobInfo jobInfo;
    private final SidecarClient sidecarClient;
    private final StorageClient storageClient;

    public BlobDataTransferApi(JobInfo jobInfo, SidecarClient sidecarClient, StorageClient storageClient)
    {
        this.jobInfo = jobInfo;
        this.sidecarClient = sidecarClient;
        this.storageClient = storageClient;
    }

    /*------ Blob APIs -------*/

    public BundleStorageObject uploadBundle(StorageCredentials writeCredentials, Bundle bundle)
    throws ClientException
    {
        try
        {
            return storageClient.multiPartUpload(writeCredentials, bundle);
        }
        catch (IOException | ExecutionException | InterruptedException exception)
        {
            rethrowOnInterruptedException("Got interrupted when uploading bundles to S3", exception);
            throw new ClientException("Failed to upload bundles to S3", exception);
        }
    }

    /*------ Sidecar APIs -------*/

    public CreateRestoreJobResponsePayload createRestoreJob(CreateRestoreJobRequestPayload createRestoreJobRequestPayload)
    throws ClientException
    {
        try
        {
            QualifiedTableName qualifiedTableName = jobInfo.qualifiedTableName();
            return sidecarClient.createRestoreJob(qualifiedTableName.keyspace(),
                                                  qualifiedTableName.table(),
                                                  createRestoreJobRequestPayload).get();
        }
        catch (ExecutionException | InterruptedException exception)
        {
            rethrowOnInterruptedException("Got interrupted when creating new restore job", exception);
            throw new ClientException("Failed to create new restore job", exception);
        }
    }

    public RestoreJobSummaryResponsePayload restoreJobSummary()
    throws ClientException
    {
        try
        {
            QualifiedTableName qualifiedTableName = jobInfo.qualifiedTableName();
            return sidecarClient.restoreJobSummary(qualifiedTableName.keyspace(),
                                                   qualifiedTableName.table(),
                                                   jobInfo.getRestoreJobId()).get();
        }
        catch (ExecutionException | InterruptedException exception)
        {
            rethrowOnInterruptedException("Got interrupted when retrieving restore job summary", exception);
            throw new ClientException("Failed to retrieve restore job summary", exception);
        }
    }

    /**
     * Called from task level to create a restore slice.
     * The request retries until the slice is created (201) or retry has exhausted.
     *
     * @param sidecarInstance           the sidecar instance where we will create the slice
     * @param createSliceRequestPayload the payload to create the slice
     * @throws ClientException when an error occurs during the slice creation
     */
    public void createRestoreSliceFromExecutor(SidecarInstance sidecarInstance,
                                               CreateSliceRequestPayload createSliceRequestPayload) throws ClientException
    {
        try
        {
            createRestoreSlice(sidecarInstance, createSliceRequestPayload, new ExecutorCreateSliceRetryPolicy())
            .get();
        }
        catch (ExecutionException | InterruptedException exception)
        {
            rethrowOnInterruptedException("Got interrupted when creating restore slice", exception);
            throw new ClientException("Failed to create restore slice for payload: " + createSliceRequestPayload,
                                      exception);
        }
    }

    /**
     * Called from driver level to create a restore slice asynchronously.
     * The request retries until the slice succeeds (200), failed (550) or retry has exhausted.
     *
     * @param sidecarInstance           the sidecar instance where we will create the slice
     * @param createSliceRequestPayload the payload to create the slice
     * @return future of create restore slice request
     */
    public CompletableFuture<Void> createRestoreSliceFromDriver(SidecarInstance sidecarInstance,
                                                                CreateSliceRequestPayload createSliceRequestPayload)
    {
        return createRestoreSlice(sidecarInstance, createSliceRequestPayload,
                                  new DriverCreateSliceRetryPolicy(sidecarClient.defaultRetryPolicy()));
    }

    /**
     * Create a restore slice with custom retry policy
     */
    private CompletableFuture<Void> createRestoreSlice(SidecarInstance sidecarInstance,
                                                       CreateSliceRequestPayload createSliceRequestPayload,
                                                       RetryPolicy retryPolicy)
    {
        QualifiedTableName qualifiedTableName = jobInfo.qualifiedTableName();
        CreateRestoreJobSliceRequest request = new CreateRestoreJobSliceRequest(qualifiedTableName.keyspace(),
                                                                                qualifiedTableName.table(),
                                                                                jobInfo.getRestoreJobId(),
                                                                                createSliceRequestPayload);
        return sidecarClient.executeRequestAsync(sidecarClient.requestBuilder()
                                                              .retryPolicy(retryPolicy)
                                                              .singleInstanceSelectionPolicy(sidecarInstance)
                                                              .request(request)
                                                              .build());
    }

    public void updateRestoreJob(UpdateRestoreJobRequestPayload updateRestoreJobRequestPayload) throws ClientException
    {
        try
        {
            QualifiedTableName qualifiedTableName = jobInfo.qualifiedTableName();
            sidecarClient.updateRestoreJob(qualifiedTableName.keyspace(),
                                           qualifiedTableName.table(),
                                           jobInfo.getRestoreJobId(),
                                           updateRestoreJobRequestPayload).get();
        }
        catch (ExecutionException | InterruptedException exception)
        {
            rethrowOnInterruptedException("Got interrupted when updating restore job", exception);
            throw new ClientException("Failed to update restore job", exception);
        }
    }

    public void abortRestoreJob() throws ClientException
    {
        try
        {
            QualifiedTableName qualifiedTableName = jobInfo.qualifiedTableName();
            sidecarClient.abortRestoreJob(qualifiedTableName.keyspace(),
                                          qualifiedTableName.table(),
                                          jobInfo.getRestoreJobId()).get();
        }
        catch (ExecutionException | InterruptedException exception)
        {
            rethrowOnInterruptedException("Got interrupted when aborting restore job", exception);
            throw new ClientException("Failed to abort restore job", exception);
        }
    }

    /**
     * {@link SidecarClient} by default retries till 200 Http response. But for create slice endpoint at task level,
     * we want to wait only till 201 Http response, hence using a custom retry policy
     */
    class ExecutorCreateSliceRetryPolicy extends RetryPolicy
    {
        @Override
        public void onResponse(CompletableFuture<HttpResponse> completableFuture,
                               Request request, HttpResponse httpResponse, Throwable throwable,
                               int attempts, boolean canRetryOnADifferentHost, RetryAction retryAction)
        {
            if (httpResponse != null && httpResponse.statusCode() == HttpResponseStatus.CREATED.code())
            {
                completableFuture.complete(httpResponse);
            }
            else
            {
                sidecarClient.defaultRetryPolicy().onResponse(completableFuture, request, httpResponse,
                                                              throwable, attempts, canRetryOnADifferentHost,
                                                              retryAction);
            }
        }
    }

    /**
     * Retry when server return CREATED 201. Besides that, its behavior is the same as what the default does.
     */
    static class DriverCreateSliceRetryPolicy extends RetryPolicy
    {
        private static final Logger LOGGER = LoggerFactory.getLogger(DriverCreateSliceRetryPolicy.class);
        private final RetryPolicy delegate;

        DriverCreateSliceRetryPolicy(RetryPolicy delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void onResponse(CompletableFuture<HttpResponse> completableFuture,
                               Request request, HttpResponse httpResponse, Throwable throwable,
                               int attempts, boolean canRetryOnADifferentHost, RetryAction retryAction)
        {
            if (httpResponse != null && httpResponse.statusCode() == HttpResponseStatus.CREATED.code())
            {
                // This is very hacky due to sidecar client is not open to modification!
                // ACCEPTED will trigger a special/unlimited retry, which is wanted here.
                // Therefore, fake a http response by setting the status code to ACCEPTED

                LOGGER.info("Received CREATED(201) for CreateSliceRequest. " +
                            "Changing the status code to ACCEPTED(202) for unlimited retry.");
                HttpResponse fakeResponseForRetry = new HttpResponseImpl(HttpResponseStatus.ACCEPTED.code(),
                                                                         httpResponse.statusMessage(),
                                                                         httpResponse.headers(),
                                                                         httpResponse.sidecarInstance());
                delegate.onResponse(completableFuture, request, fakeResponseForRetry,
                                    throwable, attempts, canRetryOnADifferentHost,
                                    retryAction);
            }
            else
            {
                delegate.onResponse(completableFuture, request, httpResponse,
                                    throwable, attempts, canRetryOnADifferentHost,
                                    retryAction);
            }
        }
    }

    private void rethrowOnInterruptedException(String message, Exception cause) throws ClientException
    {
        if (cause instanceof InterruptedException)
        {
            Thread.currentThread().interrupt();
            throw new ClientException(message, cause);
        }
    }
}
