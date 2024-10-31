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

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.transports.storage.StorageCredentialPair;
import org.apache.cassandra.spark.transports.storage.StorageCredentials;
import org.apache.cassandra.spark.transports.storage.extensions.CoordinationSignalListener;
import org.apache.cassandra.spark.transports.storage.extensions.CredentialChangeListener;
import org.apache.cassandra.spark.transports.storage.extensions.ObjectFailureListener;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportExtension;
import org.apache.spark.SparkConf;

public class LocalStorageTransportExtension implements StorageTransportExtension
{
    public static final String BUCKET_NAME = "sbw-bucket";

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStorageTransportExtension.class);

    private String jobId;
    private CoordinationSignalListener coordinationSignalListener;

    @Override
    public void initialize(String jobId, SparkConf conf, boolean isOnDriver)
    {
        this.jobId = jobId;
    }

    @Override
    public StorageTransportConfiguration getStorageConfiguration()
    {
        ImmutableMap<String, String> additionalTags = ImmutableMap.of("additional-key", "additional-value");
        return new StorageTransportConfiguration(BUCKET_NAME,
                                                 "us-west-1",
                                                 BUCKET_NAME,
                                                 "eu-west-1",
                                                 "key-prefix",
                                                 generateTokens(),
                                                 additionalTags);
    }

    @Override
    public void onTransportStart(long elapsedMillis)
    {

    }

    @Override
    public void setCredentialChangeListener(CredentialChangeListener credentialChangeListener)
    {
    }

    @Override
    public void setObjectFailureListener(ObjectFailureListener objectFailureListener)
    {
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

    private StorageCredentialPair generateTokens()
    {
        return new StorageCredentialPair("writeRegion",
                                         new StorageCredentials("writeKey",
                                                                "writeSecret",
                                                                "writeSessionToken"),
                                         "readRegion",
                                         new StorageCredentials("readKey",
                                                                "readSecret",
                                                                "readSessionToken"));
    }

    @Override
    public void onStageSucceeded(String clusterId, long elapsedMillis)
    {
        LOGGER.info("Job {} has all objects staged at cluster {} after {}ms", jobId, clusterId, elapsedMillis);
    }

    @Override
    public void onStageFailed(String clusterId, Throwable cause)
    {
        LOGGER.error("Job {} failed to stage objects at cluster {}", jobId, clusterId, cause);
    }

    @Override
    public void onImportSucceeded(String clusterId, long elapsedMillis)
    {
        LOGGER.info("Job {} has all objects imported at cluster {} after {}ms", jobId, clusterId, elapsedMillis);
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
