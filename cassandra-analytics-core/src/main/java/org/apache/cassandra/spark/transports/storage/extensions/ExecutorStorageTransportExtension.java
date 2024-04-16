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
 * Extension methods that are invoked in Spark executors only
 * Package-private interface only to be extended by {@link StorageTransportExtension}
 */
interface ExecutorStorageTransportExtension
{
    /**
     * Notifies the extension that the {@code objectURI} has been successfully persisted to the blob store.
     * This method will be called from each task during the job execution.
     *
     * @param bucket   the bucket to which the file was written
     * @param key      the key to the object written
     * @param sizeInBytes the size of the object, in bytes
     */
    void onObjectPersisted(String bucket, String key, long sizeInBytes);
}
