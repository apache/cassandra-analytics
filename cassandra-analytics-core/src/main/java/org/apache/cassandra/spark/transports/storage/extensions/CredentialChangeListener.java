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

import org.apache.cassandra.spark.transports.storage.StorageCredentialPair;
import org.jetbrains.annotations.Nullable;

/**
 * A listener interface that is notified on access token changes
 */
public interface CredentialChangeListener
{
    /**
     * The method is called when new access tokens are available for the job with ID {@code jobId}.
     * The previous set of credentials and the newly-provided set must both be valid simultaneously
     * for the Spark job to have time to rotate credentials without interruption.
     * These tokens should be provided with plenty of time for the job to distribute them to
     * the consumers of the storage transport endpoint to update their tokens before expiration.
     *
     * @param jobId     the unique identifier for the job
     * @param newTokens a map of access tokens used to authenticate to the storage transport
     */
    default void onCredentialsChanged(String jobId, StorageCredentialPair newTokens)
    {
        onCredentialsChanged(jobId, null, newTokens);
    }

    /**
     * The method is called when new access tokens are available for the job with ID {@code jobId}
     * in the cluster of {@code clusterId}.
     * The previous set of credentials and the newly-provided set must both be valid simultaneously
     * for the Spark job to have time to rotate credentials without interruption.
     * These tokens should be provided with plenty of time for the job to distribute them to
     * the consumers of the storage transport endpoint to update their tokens before expiration.
     *
     * @param jobId     the unique identifier for the job
     * @param clusterId the unique identifier for the cluster.
     *                  When its value is null, it behaves the same as {@link #onCredentialsChanged(String, StorageCredentialPair)}
     * @param newTokens a map of access tokens used to authenticate to the storage transport
     */
    void onCredentialsChanged(String jobId, @Nullable String clusterId, StorageCredentialPair newTokens);
}
