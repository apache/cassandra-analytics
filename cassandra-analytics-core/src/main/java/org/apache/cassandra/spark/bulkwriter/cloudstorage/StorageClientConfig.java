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

package org.apache.cassandra.spark.bulkwriter.cloudstorage;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageClientConfig implements Serializable
{
    private static final long serialVersionUID = -1572678388713210328L;
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageClientConfig.class);

    public final String threadNamePrefix;
    // Controls the max concurrency/parallelism of the thread pool used by s3 client
    public final int concurrency;
    // Controls the timeout of idle threads
    public final long threadKeepAliveSeconds;
    public final int maxChunkSizeInBytes;
    public final URI httpsProxy; // optional; configures https proxy for s3 client
    public final long nioHttpClientConnectionAcquisitionTimeoutSeconds; // optional; only applied for NettyNioHttpClient
    public final int nioHttpClientMaxConcurrency; // optional; only applied for NettyNioHttpClient

    public final URI endpointOverride; // nullable; only used for testing.

    public StorageClientConfig(int concurrency,
                               long threadKeepAliveSeconds,
                               int maxChunkSizeInBytes,
                               String httpsProxy,
                               String endpointOverride,
                               long nioHttpClientConnectionAcquisitionTimeoutSeconds,
                               int nioHttpClientMaxConcurrency)
    {
        this.threadNamePrefix = "storage-client";
        this.concurrency = concurrency;
        this.threadKeepAliveSeconds = threadKeepAliveSeconds;
        this.maxChunkSizeInBytes = maxChunkSizeInBytes;
        this.httpsProxy = toURI(httpsProxy, "HttpsProxy");
        this.endpointOverride = toURI(endpointOverride, "EndpointOverride");
        this.nioHttpClientConnectionAcquisitionTimeoutSeconds = nioHttpClientConnectionAcquisitionTimeoutSeconds;
        this.nioHttpClientMaxConcurrency = nioHttpClientMaxConcurrency;
    }

    private URI toURI(String uriString, String hint)
    {
        if (uriString == null)
        {
            return null;
        }

        try
        {
            return new URI(uriString);
        }
        catch (URISyntaxException e)
        {
            LOGGER.error("{} is specified, but the value is invalid. input={}", hint, uriString);
            throw new RuntimeException("Unable to resolve " + uriString, e);
        }
    }
}
