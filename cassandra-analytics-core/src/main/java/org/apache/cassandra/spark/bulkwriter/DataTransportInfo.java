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

public class DataTransportInfo implements Serializable
{
    private static final long serialVersionUID = 1823178559314014761L;
    private final DataTransport transport;
    private final String transportExtensionClass;

    // note this should be shifted under appropriate class
    private final long maxSizePerBundleInBytes;

    public DataTransportInfo(DataTransport transport, String transportExtensionClass, long maxSizePerBundleInBytes)
    {
        this.transport = transport;
        this.transportExtensionClass = transportExtensionClass;
        this.maxSizePerBundleInBytes = maxSizePerBundleInBytes;
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

    public String toString()
    {
        return "TransportInfo={dataTransport=" + transport
               + ",transportExtensionClass=" + transportExtensionClass
               + ",maxSizePerBundleInBytes=" + maxSizePerBundleInBytes + '}';
    }
}
