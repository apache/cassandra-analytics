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

package org.apache.cassandra.spark.example;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.util.ThreadUtil;
import org.apache.cassandra.spark.transports.storage.StorageCredentialPair;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;
import org.apache.cassandra.spark.transports.storage.extensions.CoordinationSignalListener;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;
import org.apache.cassandra.spark.transports.storage.extensions.ObjectFailureListener;
import org.apache.cassandra.spark.transports.storage.extensions.CredentialChangeListener;
import org.apache.spark.SparkConf;

public class ExampleStorageTransportExtension implements StorageTransportExtension
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExampleStorageTransportExtension.class);

    private SparkConf conf;
    private ScheduledExecutorService scheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(ThreadUtil.threadFactory("ExampleBlobStorageOperations"));
    private String jobId;
    private long tokenCount = 0;
    private CredentialChangeListener credentialChangeListener;
    private ObjectFailureListener objectFailureListener;
    private CoordinationSignalListener coordinationSignalListener;
    private boolean shouldFail;

    @Override
    public void initialize(String jobId, SparkConf conf, boolean isOnDriver)
    {
        this.jobId = jobId;
        this.conf = conf;
        this.shouldFail = conf.getBoolean("blob_operations_should_fail", false);
    }

    @Override
    public StorageTransportConfiguration getStorageConfiguration()
    {
        ImmutableMap<String, String> additionalTags = ImmutableMap.of("additional-key", "additional-value");
        return new StorageTransportConfiguration("writebucket-name",
                                                 "us-west-2",
                                                 "readbucket-name",
                                                 "eu-west-1",
                                                 "some-prefix-for-each-job",
                                                 generateTokens(this.tokenCount++),
                                                 additionalTags);
    }

    @Override
    public void onTransportStart(long elapsedMillis)
    {

    }

    @Override
    public void setCredentialChangeListener(CredentialChangeListener credentialChangeListener)
    {
        LOGGER.info("Token listener registered for job {}", jobId);
        this.credentialChangeListener = credentialChangeListener;
        startFakeTokenRefresh();
    }

    @Override
    public void setObjectFailureListener(ObjectFailureListener objectFailureListener)
    {
        this.objectFailureListener = objectFailureListener;
        if (this.shouldFail)
        {
            scheduledExecutorService.schedule(this::fail, 1, TimeUnit.SECONDS);
        }
    }

    private void fail()
    {
        this.objectFailureListener.onObjectFailed(this.jobId, "failed_bucket", "failed_key", "Fake failure");
    }

    @Override
    public void onObjectPersisted(String bucket, String key, long sizeInBytes)
    {
        LOGGER.info("Object {}/{} for job {} persisted with size {} bytes", bucket, key, jobId, sizeInBytes);
    }

    @Override
    public void onAllObjectsPersisted(long objectsCount, long rowCount, long elapsedMillis)
    {
        LOGGER.info("All {} objects, totaling {} rows, are persisted with elapsed time {}ms",
                    objectsCount, rowCount, elapsedMillis);
    }

    @Override
    public void onObjectApplied(String bucket, String key, long sizeInBytes, long elapsedMillis)
    {

    }

    @Override
    public void onJobSucceeded(long elapsedMillis)
    {
        LOGGER.info("Job {} succeeded with elapsed time {}ms", jobId, elapsedMillis);
    }

    @Override
    public void onJobFailed(long elapsedMillis, Throwable throwable)
    {
        LOGGER.error("Job {} failed after {}ms", jobId, elapsedMillis, throwable);
    }

    private void startFakeTokenRefresh()
    {
        scheduledExecutorService.scheduleAtFixedRate(this::refreshTokens, 1, 1, TimeUnit.SECONDS);

    }

    private void refreshTokens()
    {
        this.credentialChangeListener.onCredentialsChanged(this.jobId, generateTokens(this.tokenCount++));
    }

    private StorageCredentialPair generateTokens(long tokenCount)
    {
        return new StorageCredentialPair("writeRegion",
                                         new StorageCredentials("writeAccessKeyId-" + tokenCount,
                                                                "writeSecretKey-" + tokenCount,
                                                                "writeSessionToken-" + tokenCount),
                                         "readRegion",
                                         new StorageCredentials(
                                         "readAccessKeyId-" + tokenCount,
                                         "readSecretKey-" + tokenCount,
                                         "readSessionToken-" + tokenCount));
    }

    @Override
    public void onStageSucceeded(String clusterId, long elapsedMillis)
    {
        LOGGER.info("Job {} has all objects staged at cluster {} after {}ms", jobId, clusterId, elapsedMillis);
    }

    @Override
    public void onStageFailed(String clusterId, Throwable cause)
    {
        LOGGER.error("Cluster {} failed to stage objects", clusterId, cause);
    }

    @Override
    public void onImportSucceeded(String clusterId, long elapsedMillis)
    {
        LOGGER.info("Job {} has all objects applied at cluster {} after {}ms", jobId, clusterId, elapsedMillis);
    }

    @Override
    public void onImportFailed(String clusterId, Throwable cause)
    {
        LOGGER.error("Cluster {} failed to apply objects", clusterId, cause);
    }

    @Override
    public void setCoordinationSignalListener(CoordinationSignalListener listener)
    {
        this.coordinationSignalListener = listener;
        LOGGER.info("CoordinationSignalListener initialized. listener={}", listener);
    }
}
