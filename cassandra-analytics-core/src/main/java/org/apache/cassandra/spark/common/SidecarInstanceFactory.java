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

package org.apache.cassandra.spark.common;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import o.a.c.sidecar.client.shaded.client.SidecarInstanceImpl;

public class SidecarInstanceFactory
{
    private SidecarInstanceFactory()
    {
        throw new UnsupportedOperationException("Utility class");
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarInstanceFactory.class);

    /**
     * Create SidecarInstance object by parsing the input string, which is IP address or hostname and optionally includes port
     * <p>The input may also carry an optional per-instance id as a trailing {@code "=<id>"} suffix, e.g.
     * {@code "host:9043=2"}. The id identifies which local Cassandra instance the receiving Sidecar should route
     * requests to; it is used to populate the {@code instanceId} query parameter per instance instead of relying on a
     * single job-level value. {@code '='} cannot appear in a hostname, IPv4/IPv6 address or port, so the suffix is
     * unambiguous. When absent, requests fall back to the job-level {@code instanceId}, if any.
     * @param input hostname string that can optionally includes the port. If port is present, the defaultPort param is ignored.
     * @param defaultPort port value used when the input string contains no port
     * @return SidecarInstanceImpl
     */
    public static SidecarInstanceImpl createFromString(String input, int defaultPort)
    {
        Preconditions.checkArgument(StringUtils.isNotEmpty(input), "Unable to create sidecar instance from empty input");

        String address = input;
        Integer instanceId = null;
        // Optional per-instance id, expressed as a trailing "=<id>" suffix (e.g. "host:9043=2").
        int equalsIndex = input.lastIndexOf('=');
        if (equalsIndex >= 0)
        {
            String instanceIdStr = input.substring(equalsIndex + 1).trim();
            try
            {
                instanceId = Integer.parseInt(instanceIdStr);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException(
                String.format("Invalid sidecar instanceId '%s' in '%s'; expected a non-negative integer", instanceIdStr, input), e);
            }
            Preconditions.checkArgument(instanceId >= 0, "Sidecar instanceId must be non-negative; got %s in '%s'", instanceId, input);
            address = input.substring(0, equalsIndex);
        }

        String hostname = address;
        int port = defaultPort;
        // has port in the string. The former matches ipv6 and the latter matches ipv4 and hostnames
        // ipv6 with port example: [2024:a::1]:8080
        if (address.contains("]:") || (!address.startsWith("[") && address.contains(":")))
        {
            int index = address.lastIndexOf(':');
            hostname = address.substring(0, index); // includes ']' if it is ipv6
            String portStr = address.substring(index + 1);
            port = Integer.parseInt(portStr);
        }

        Preconditions.checkState(port != -1, "Unable to resolve port from %s", input);

        LOGGER.info("Create sidecar instance. hostname={} port={} instanceId={}", hostname, port, instanceId);
        return new SidecarInstanceImpl(hostname, port, instanceId);
    }

    /**
     * Similar to {@link SidecarInstanceFactory#createFromString(String, int)}, but it requires that the input string must include port
     * @param hostnameWithPort hostname with port
     * @return SidecarInstanceImpl
     */
    public static SidecarInstanceImpl createFromString(String hostnameWithPort)
    {
        return createFromString(hostnameWithPort, -1);
    }
}
