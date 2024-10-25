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

package org.apache.cassandra.spark.transports.storage.extensions;

import java.util.Objects;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.data.RestoreJobSecrets;
import o.a.c.sidecar.client.shaded.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.spark.bulkwriter.CancelJobEvent;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.TransportContext;
import org.apache.cassandra.spark.bulkwriter.cloudstorage.coordinated.CoordinatedCloudStorageDataTransferApi;
import org.apache.cassandra.spark.transports.storage.StorageCredentialPair;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.spark.transports.storage.extensions.TransportExtensionUtils.validateReceivedJobId;

public class StorageTransportHandler implements CredentialChangeListener, ObjectFailureListener
{
    private final TransportContext.CloudStorageTransportContext transportContext;
    private final Consumer<CancelJobEvent> cancelConsumer;
    private final JobInfo jobInfo;

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageTransportHandler.class);

    public StorageTransportHandler(TransportContext.CloudStorageTransportContext transportContext,
                                   JobInfo jobInfo,
                                   Consumer<CancelJobEvent> cancelConsumer)
    {
        this.transportContext = transportContext;
        this.jobInfo = jobInfo;
        this.cancelConsumer = cancelConsumer;
    }

    @Override
    public void onCredentialsChanged(String jobId, @Nullable String clusterId, StorageCredentialPair newCredentials)
    {
        validateReceivedJobId(jobId, jobInfo);
        Preconditions.checkState(!jobInfo.isCoordinatedWriteEnabled() || clusterId != null,
                                 "ClusterId cannot be null for coordinated write enabled jobs");
        if (Objects.equals(transportContext.transportConfiguration().getStorageCredentialPair(clusterId), newCredentials))
        {
            LOGGER.info("The received credentials have not changed. Skip updating. clusterId={}", clusterId);
            return;
        }

        LOGGER.info("Refreshing cloud storage credentials. jobId={} credentials={} clusterId={}", jobId, newCredentials, clusterId);
        transportContext.transportConfiguration().setStorageCredentialPair(clusterId, newCredentials);
        updateCredentials(clusterId, newCredentials);
    }

    @Override
    public void onObjectFailed(String jobId, String bucket, String key, String errorMessage)
    {
        validateReceivedJobId(jobId, jobInfo);
        LOGGER.error("Object with bucket {} and key {} failed to be transported correctly. Cancelling job. Error was: {}", bucket, key, errorMessage);
        cancelConsumer.accept(new CancelJobEvent(errorMessage));
    }

    private void updateCredentials(@Nullable String clusterId, StorageCredentialPair credentialPair)
    {
        RestoreJobSecrets secrets = credentialPair.toRestoreJobSecrets();
        UpdateRestoreJobRequestPayload requestPayload = UpdateRestoreJobRequestPayload.builder().withSecrets(secrets).build();
        if (clusterId != null)
        {
            CoordinatedCloudStorageDataTransferApi dataTransferApi = (CoordinatedCloudStorageDataTransferApi) transportContext.dataTransferApi();
            dataTransferApi.getValueOrThrow(clusterId).updateRestoreJob(requestPayload);
        }
        else
        {
            transportContext.dataTransferApi().updateRestoreJob(requestPayload);
        }
    }
}
