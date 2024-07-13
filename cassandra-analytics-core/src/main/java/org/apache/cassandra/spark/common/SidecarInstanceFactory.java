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

import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;

public class SidecarInstanceFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarInstanceFactory.class);

    public static SidecarInstanceImpl createFromString(String input, int defaultPort)
    {
        Preconditions.checkArgument(StringUtils.isNotEmpty(input), "Unable to create sidecar instance from empty input");

        String hostname = input;
        int port = defaultPort;
        // has port in the string. The former matches ipv6 and the latter matches ipv4 and hostnames
        // ipv6 with port example: [2024:a::1]:8080
        if (input.contains("]:") || (!input.startsWith("[") && input.contains(":")))
        {
            int index = input.lastIndexOf(':');
            hostname = input.substring(0, index); // includes ']' if it is ipv6
            String portStr = input.substring(index + 1);
            port = Integer.parseInt(portStr);
        }

        LOGGER.info("Create sidecar instance. hostname={} port={}", hostname, port);
        return new SidecarInstanceImpl(hostname, port);
    }
}
