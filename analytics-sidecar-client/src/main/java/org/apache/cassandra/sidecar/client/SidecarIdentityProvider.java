/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.client;

import java.util.Map;

import org.jetbrains.annotations.ApiStatus;

/**
 * An extension point for a custom identity provider for Sidecar communication.
 *
 * <p>An identity provider is registered by specifying its fully qualified class name
 * in the {@code sidecar_identity_provider_class} property. Implementations must
 * provide a no-argument constructor. Initialization code should be placed in the
 * {@link #initialize(Map, HttpClient)} method.
 *
 * <p>Implementations must be thread-safe, as methods may be invoked concurrently from
 * multiple worker threads. Avoid performing long-running I/O interactions inside the
 * {@link #injectCredentials(RequestContext.Builder)} callback.
 */
@ApiStatus.Experimental
public interface SidecarIdentityProvider
{
    SidecarIdentityProvider NOOP = requestBuilder -> {};

    /**
     * Initializes the identity provider. This method can be invoked from both the Spark driver and executors.
     *
     * @param options    the identity provider's options passed as part of the Spark configuration,
     *                   prefixed by {@code sidecar_identity_provider_parameter.}.
     *                   For example, configuring {@code "sidecar_identity_provider_parameter.param1" = "value1"}
     *                   will result in a map entry of {@code "param1" = "value1"}.
     * @param httpClient the HTTP client used for communication
     */
    default void initialize(Map<String, String> options, HttpClient httpClient)
    {
    }

    /**
     * Callback executed before a request is built and sent to Sidecar.
     * A typical implementation will inject custom HTTP headers or credentials into the request.
     *
     * @param requestBuilder the request builder used to construct the upcoming request
     */
    void injectCredentials(RequestContext.Builder requestBuilder);
}
