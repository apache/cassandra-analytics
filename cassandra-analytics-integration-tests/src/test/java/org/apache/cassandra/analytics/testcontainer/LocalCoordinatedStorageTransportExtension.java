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

package org.apache.cassandra.analytics.testcontainer;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.spark.transports.storage.StorageAccessConfiguration;
import org.apache.cassandra.spark.transports.storage.StorageCredentialPair;
import org.apache.cassandra.spark.transports.storage.extensions.CoordinationSignalListener;
import org.apache.cassandra.spark.transports.storage.extensions.StorageTransportConfiguration;
import org.apache.spark.SparkConf;

import static org.apache.cassandra.analytics.testcontainer.BulkWriteS3CompatModeSimpleTest.BUCKET_NAME;

/**
 * Used by {@link CoordinatedBulkWriteSimpleTest}
 */
public class LocalCoordinatedStorageTransportExtension extends LocalStorageTransportExtension
{
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
        StorageCredentialPair tokens = generateTokens();
        return new StorageTransportConfiguration("key-prefix",
                                                 ImmutableMap.of(),
                                                 new StorageAccessConfiguration("writeRegion", BUCKET_NAME, tokens.writeCredentials),
                                                 ImmutableMap.of(
                                                 "cluster1",
                                                 new StorageAccessConfiguration("readRegion1", BUCKET_NAME, tokens.readCredentials),
                                                 "cluster2",
                                                 new StorageAccessConfiguration("readRegion2", BUCKET_NAME, tokens.readCredentials)));
    }

    @Override
    public void setCoordinationSignalListener(CoordinationSignalListener listener)
    {
        this.coordinationSignalListener = listener;
    }

    @Override
    public void onAllObjectsPersisted(long objectsCount, long rowCount, long elapsedMillis)
    {
        coordinationSignalListener.onStageReady(jobId);
    }

    @Override
    public void onStageSucceeded(String clusterId, long elapsedMillis)
    {
        coordinationSignalListener.onImportReady(jobId);
    }
}
