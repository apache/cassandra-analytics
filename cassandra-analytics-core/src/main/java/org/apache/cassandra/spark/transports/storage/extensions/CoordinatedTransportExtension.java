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

import org.apache.cassandra.spark.bulkwriter.CassandraBulkSourceRelation;

/**
 * Extension methods that enables coordinated write to multiple target clusters
 * Package-private interface only to be extended by {@link StorageTransportExtension}
 * <p>
 * Note that the methods defined in this extension run in Spark Driver only
 * <p>
 * The coordinated write has 2 phases, i.e. staging phase and importing phase. In the happy path, the steps of a run are the following:
 * <ol>
 *     <li>Extension sets the {@link CoordinationSignalListener} on initialization.</li>
 *     <li>Extension invokes {@link CoordinationSignalListener#onStageReady(String)},
 *     when it decides it is time to stage SSTables on all clusters.</li>
 *     <li>Cassandra Analytics calls Sidecars to stage data.
 *     {@link #onStageSucceeded(String, long)} is called per cluster to notify the extension.</li>
 *     <li>Extension invokes {@link CoordinationSignalListener#onImportReady(String)},
 *     when it decides it is time to apply/import SSTables on all clusters.</li>
 *     <li>Cassandra Analytics calls Sidecars to import data.
 *     {@link #onImportSucceeded(String, long)} is called per cluster to notify the extension.</li>
 *     <li>{@link DriverStorageTransportExtension#onAllObjectsPersisted(long, long, long)}
 *     is called to indicate the completion.</li>
 * </ol>
 */
interface CoordinatedTransportExtension
{
    /**
     * Notifies the {@link CoordinatedTransportExtension} implementation that all objects have been staged on the cluster.
     * The callback should only be invoked once per cluster
     *
     * @param clusterId identifies a Cassandra cluster
     * @param elapsedMillis the elapsed time from the start of the bulk write run in milliseconds
     */
    void onStageSucceeded(String clusterId, long elapsedMillis);

    /**
     * Notifies the {@link CoordinatedTransportExtension} implementation that it fails to stage objects on the cluster.
     * The callback should only be invoked once per cluster
     *
     * @param clusterId identifies a Cassandra cluster
     * @param cause failure
     */
    void onStageFailed(String clusterId, Throwable cause);

    /**
     * Notifies the {@link CoordinatedTransportExtension} implementation that all objects have been imported into the cluster.
     * The callback should only be invoked once per cluster
     *
     * @param clusterId identifies a Cassandra cluster
     * @param elapsedMillis the elapsed time from the start of the bulk write run in milliseconds
     */
    void onImportSucceeded(String clusterId, long elapsedMillis);

    /**
     * Notifies the {@link CoordinatedTransportExtension} implementation that it fails to import objects into the cluster.
     * The callback should only be invoked once per cluster
     *
     * @param clusterId identifies a Cassandra cluster
     * @param cause failure
     */
    void onImportFailed(String clusterId, Throwable cause);

    /**
     * Set the {@link CoordinationSignalListener} to receive coordination signals from {@link CoordinatedTransportExtension} implementation
     * <p>
     * Note to {@link CoordinatedTransportExtension} implementor:
     * this method is called during setup of {@link CassandraBulkSourceRelation}, and a {@link CoordinationSignalListener} instance is provided
     *
     * @param listener receives coordination signals
     */
    void setCoordinationSignalListener(CoordinationSignalListener listener);
}
