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

import java.util.function.BiConsumer;
import java.util.function.Predicate;

import o.a.c.sidecar.client.shaded.common.data.RestoreJobProgressFetchPolicy;
import o.a.c.sidecar.client.shaded.common.request.data.CreateSliceRequestPayload;
import o.a.c.sidecar.client.shaded.common.response.data.RestoreJobProgressResponsePayload;
import org.apache.cassandra.spark.exception.SidecarApiCallException;

/**
 * Additional APIs to support coordinated write
 */
public interface CoordinatedCloudStorageDataTransferApiExtension
{
    /*------ Sidecar APIs -------*/


    /**
     * Called from task level to create a restore slice.
     * The request retries until the slice is created (201) or retry has exhausted.
     *
     * @param clusterId identifier of the cluster to create slice
     * @param createSliceRequestPayload the payload to create the slice
     * @throws SidecarApiCallException when an error occurs during the slice creation
     */
    void createRestoreSliceFromExecutor(String clusterId, CreateSliceRequestPayload createSliceRequestPayload) throws SidecarApiCallException;

    /**
     * Retrieve the restore job progress with the specified fetch policy and handle the progress response
     * <p>
     * The API is invoked by Spark driver.
     * @param fetchPolicy determines how detailed is the fetched restore job progress
     * @param clusterIdFilter filter to determine whether a cluster should be skipped
     * @param progressHandler handles restore job progress
     * @throws SidecarApiCallException exception from calling sidecar API
     */
    void restoreJobProgress(RestoreJobProgressFetchPolicy fetchPolicy,
                            Predicate<String> clusterIdFilter,
                            BiConsumer<String, RestoreJobProgressResponsePayload> progressHandler) throws SidecarApiCallException;
}
