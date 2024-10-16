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

import java.util.concurrent.CompletableFuture;

import o.a.c.sidecar.client.shaded.common.request.data.CreateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.request.data.CreateSliceRequestPayload;
import o.a.c.sidecar.client.shaded.common.request.data.UpdateRestoreJobRequestPayload;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.spark.exception.S3ApiCallException;
import org.apache.cassandra.spark.exception.SidecarApiCallException;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;

/**
 * The collection of APIs for cloud-storage-based data transfer
 */
public interface CloudStorageDataTransferApi
{
    /*------ Cloud storage APIs -------*/

    BundleStorageObject uploadBundle(StorageCredentials writeCredentials, Bundle bundle) throws S3ApiCallException;


    /*------ Sidecar APIs -------*/

    void createRestoreJob(CreateRestoreJobRequestPayload createRestoreJobRequestPayload) throws SidecarApiCallException;

    RestoreJobSummaryResponsePayload restoreJobSummary() throws SidecarApiCallException;

    /**
     * Called from task level to create a restore slice.
     * The request retries until the slice is created (201) or retry has exhausted.
     *
     * @param sidecarInstance           the sidecar instance where we will create the slice
     * @param createSliceRequestPayload the payload to create the slice
     * @throws SidecarApiCallException when an error occurs during the slice creation
     */
    void createRestoreSliceFromExecutor(SidecarInstance sidecarInstance,
                                        CreateSliceRequestPayload createSliceRequestPayload) throws SidecarApiCallException;

    /**
     * Called from driver level to create a restore slice asynchronously.
     * The request retries until the slice succeeds (200), failed (550) or retry has exhausted.
     *
     * @param sidecarInstance           the sidecar instance where we will create the slice
     * @param createSliceRequestPayload the payload to create the slice
     * @return future of create restore slice request
     */
    CompletableFuture<Void> createRestoreSliceFromDriver(SidecarInstance sidecarInstance,
                                                         CreateSliceRequestPayload createSliceRequestPayload);

    void updateRestoreJob(UpdateRestoreJobRequestPayload updateRestoreJobRequestPayload) throws SidecarApiCallException;

    void abortRestoreJob() throws SidecarApiCallException;

    default void handleInterruption(Exception cause)
    {
        if (cause instanceof InterruptedException)
        {
            Thread.currentThread().interrupt();
        }
    }
}
