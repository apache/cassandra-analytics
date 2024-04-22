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
import java.util.UUID;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.common.data.RestoreJobSecrets;
import o.a.c.sidecar.client.shaded.common.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.spark.bulkwriter.CancelJobEvent;
import org.apache.cassandra.spark.bulkwriter.JobInfo;
import org.apache.cassandra.spark.bulkwriter.TransportContext;
import org.apache.cassandra.spark.common.client.ClientException;
import org.apache.cassandra.spark.transports.storage.StorageCredentialPair;

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
    public void onCredentialsChanged(String jobId, StorageCredentialPair newCredentials)
    {
        validateReceivedJobId(jobId);
        if (Objects.equals(transportContext.transportConfiguration().getStorageCredentialPair(), newCredentials))
        {
            LOGGER.info("The received new credential is the same as the existing one. Skip updating.");
            return;
        }

        LOGGER.info("Refreshing cloud storage credentials. jobId={}, credentials={}", jobId, newCredentials);
        transportContext.transportConfiguration().setBlobCredentialPair(newCredentials);
        updateCredentials(jobInfo.getRestoreJobId(), newCredentials);
    }

    @Override
    public void onObjectFailed(String jobId, String bucket, String key, String errorMessage)
    {
        validateReceivedJobId(jobId);
        LOGGER.error("Object with bucket {} and key {} failed to be transported correctly. Cancelling job. Error was: {}", bucket, key, errorMessage);
        cancelConsumer.accept(new CancelJobEvent(errorMessage));
    }

    private void updateCredentials(UUID jobId, StorageCredentialPair credentialPair)
    {
        StorageTransportConfiguration conf = transportContext.transportConfiguration();
        RestoreJobSecrets secrets = credentialPair.toRestoreJobSecrets(conf.getReadRegion(), conf.getWriteRegion());
        UpdateRestoreJobRequestPayload requestPayload = new UpdateRestoreJobRequestPayload(null, secrets, null, null);
        try
        {
            transportContext.dataTransferApi().updateRestoreJob(requestPayload);
        }
        catch (ClientException e)
        {
            throw new RuntimeException("Failed to update secretes for restore job. restoreJobId: " + jobId, e);
        }
    }

    private void validateReceivedJobId(String jobId)
    {
        String actualJobId = jobInfo.getId();
        if (!Objects.equals(jobId, actualJobId))
        {
            throw new IllegalStateException("Received jobId does not match with the actual one. Received: " + jobId
                                            + "; actual: " + actualJobId);
        }
    }
}
