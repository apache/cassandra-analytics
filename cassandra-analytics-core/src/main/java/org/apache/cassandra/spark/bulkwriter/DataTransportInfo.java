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

package org.apache.cassandra.spark.bulkwriter;

import java.io.Serializable;

import org.jetbrains.annotations.Nullable;

public class DataTransportInfo implements Serializable
{
    private static final long serialVersionUID = 1823178559314014761L;
    private final DataTransport transport;
    private final String transportExtensionClass;

    // note this should be shifted under appropriate class
    private final long maxSizePerBundleInBytes;

    /**
     * The credential type name (e.g. "STATIC", "IAM"), stored as a String to avoid tight coupling to
     * shaded sidecar classes and to stay safely serializable across Spark broadcast. Null means the user
     * did not specify one, and the sidecar will use its default (STATIC).
     */
    @Nullable
    private final String storageCredentialType;

    public DataTransportInfo(DataTransport transport,
                             String transportExtensionClass,
                             long maxSizePerBundleInBytes)
    {
        this(transport, transportExtensionClass, maxSizePerBundleInBytes, null);
    }

    public DataTransportInfo(DataTransport transport,
                             String transportExtensionClass,
                             long maxSizePerBundleInBytes,
                             @Nullable String storageCredentialType)
    {
        this.transport = transport;
        this.transportExtensionClass = transportExtensionClass;
        this.maxSizePerBundleInBytes = maxSizePerBundleInBytes;
        this.storageCredentialType = storageCredentialType;
    }

    public DataTransport getTransport()
    {
        return transport;
    }

    public String getTransportExtensionClass()
    {
        return transportExtensionClass;
    }

    public long getMaxSizePerBundleInBytes()
    {
        return maxSizePerBundleInBytes;
    }

    @Nullable
    public String getStorageCredentialType()
    {
        return storageCredentialType;
    }

    public String toString()
    {
        return "TransportInfo={dataTransport=" + transport
               + ",transportExtensionClass=" + transportExtensionClass
               + ",maxSizePerBundleInBytes=" + maxSizePerBundleInBytes
               + ",storageCredentialType=" + storageCredentialType + '}';
    }
}
