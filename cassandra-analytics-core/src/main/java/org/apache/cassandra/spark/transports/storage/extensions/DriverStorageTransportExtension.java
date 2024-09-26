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

/**
 * Extension methods that are invoked in Spark driver only
 * Package-private interface only to be extended by {@link StorageTransportExtension}
 */
interface DriverStorageTransportExtension extends CoordinatedTransportExtension
{
    /**
     * Notifies the extension that data transport has been started. This method will be called from the driver.
     * @param elapsedMillis the elapsed time from the start of the bulk write run until this step for the job in milliseconds
     */
    void onTransportStart(long elapsedMillis);

    /**
     * Sets the {@link CredentialChangeListener} to listen for token changes. This method
     * will be called from the driver.
     *
     * @param credentialChangeListener an implementation of the {@link CredentialChangeListener}
     */
    void setCredentialChangeListener(CredentialChangeListener credentialChangeListener);

    /**
     * Sets the {@link ObjectFailureListener} to listen for token changes. This method
     * will be called from the driver.
     *
     * @param objectFailureListener an implementation of the {@link ObjectFailureListener}
     */
    void setObjectFailureListener(ObjectFailureListener objectFailureListener);

    /**
     * Notifies the extension that all the objects have been persisted to the cloud storage successfully.
     * This method is called from driver when all executor tasks complete.
     *
     * @param objectsCount the total count of objects persisted
     * @param rowCount the total count of rows persisted
     * @param elapsedMillis the elapsed time from the start of the bulk write run until this step for the job in milliseconds
     */
    void onAllObjectsPersisted(long objectsCount, long rowCount, long elapsedMillis);

    /**
     * Notifies the extension that the object identified by the bucket and key has been applied, meaning
     * the SSTables included in the object is imported into Cassandra and satisfies the desired consistency level.
     * <br>
     * The notification is only emitted once per object and as soon as the consistency level is satisfied.
     *
     * @param bucket the belonging bucket of the object
     * @param key the object key
     * @param sizeInBytes the size of the object in bytes
     * @param elapsedMillis the elapsed time from the start of the bulk write run until this step for the job in milliseconds
     */
    void onObjectApplied(String bucket, String key, long sizeInBytes, long elapsedMillis);

    /**
     * Notifies the extension that the job has completed successfully. This method will be called
     * from the driver at the end of the Spark Bulk Writer execution when the job succeeds.
     *
     * @param elapsedMillis the elapsed time from the start of the bulk write run until this step for the job in milliseconds
     */
    void onJobSucceeded(long elapsedMillis);

    /**
     * Notifies the extension that the job has failed with exception {@link Throwable throwable}.
     * This method will be called from the driver at the end of the Spark Bulk Writer execution when the job fails.
     *
     * @param elapsedMillis the elapsed time from the start of the bulk write run until this step for the job in milliseconds
     * @param throwable     the exception encountered by the job
     */
    void onJobFailed(long elapsedMillis, Throwable throwable);
}
