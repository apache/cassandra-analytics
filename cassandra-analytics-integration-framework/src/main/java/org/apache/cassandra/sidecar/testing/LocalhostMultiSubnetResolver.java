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

package org.apache.cassandra.sidecar.testing;

import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolvers;

/**
 * A {@link DnsResolver} instance used for tests that provides fast DNS resolution, to avoid blocking
 * DNS resolution at the JDK/OS-level.
 *
 * <p><b>NOTE:</b> The resolver assumes that the addresses are of the form 127.0.x.y, which is what is currently
 * configured for integration tests.
 */
public class LocalhostMultiSubnetResolver implements DnsResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalhostMultiSubnetResolver.class);
    private static final Pattern HOSTNAME_PATTERN = Pattern.compile("^local(\\d+)+host(\\d+)+$");
    private final DnsResolver delegate;
    private final int subnet;

    public LocalhostMultiSubnetResolver()
    {
        this(DnsResolvers.DEFAULT, 0);
    }

    public LocalhostMultiSubnetResolver(int subnet)
    {
        this(DnsResolvers.DEFAULT, subnet);
    }

    LocalhostMultiSubnetResolver(DnsResolver delegate, int subnet)
    {
        this.delegate = delegate;
        this.subnet = subnet;
    }

    /**
     * Returns the resolved IP address from the hostname. If the {@code hostname} pattern is not matched,
     * delegate the resolution to the delegate resolver.
     *
     * <pre>
     * resolver.resolve("local0host1") = "127.0.0.1"
     * resolver.resolve("local0host2") = "127.0.0.2"
     * resolver.resolve("local1host1") = "127.0.1.1"
     * resolver.resolve("127.0.0.5") = "127.0.0.5"
     * </pre>
     *
     * @param hostname the hostname to resolve
     * @return the resolved IP address
     */
    @Override
    public String resolve(String hostname) throws UnknownHostException
    {
        Matcher matcher = HOSTNAME_PATTERN.matcher(hostname);
        if (!matcher.matches())
        {
            LOGGER.warn("Invalid hostname found {}.", hostname);
            return delegate.resolve(hostname);
        }
        String subnet = matcher.group(1);
        String host = matcher.group(2);
        return "127.0." + subnet + "." + host;
    }

    /**
     * Returns the resolved hostname from the given {@code address}. When an invalid IP address is provided,
     * delegates {@code address} resolution to the delegate.
     *
     * <pre>
     * resolver.reverseResolve("127.0.0.1") = "local0host1"
     * resolver.reverseResolve("127.0.0.2") = "local0host2"
     * resolver.reverseResolve("127.0.1.1") = "local1host1"
     * resolver.reverseResolve("localhost5") = "localhost5"
     * </pre>
     *
     * @param address the IP address to perform the reverse resolution
     * @return the resolved hostname for the given {@code address}
     */
    @Override
    public String reverseResolve(String address) throws UnknownHostException
    {
        // IP addresses have the form 127.0.x.y
        int lastDotIndex = address.lastIndexOf('.');
        if (lastDotIndex < 0 || lastDotIndex + 1 == address.length())
        {
            LOGGER.warn("Invalid ip address found {}.", address);
            return delegate.reverseResolve(address);
        }
        String netNumber = address.substring(lastDotIndex + 1);
        String subnet = address.substring(0, lastDotIndex);
        int subnetDotIndex = subnet.lastIndexOf('.');
        String subnetNumber = subnet.substring(subnetDotIndex + 1, lastDotIndex);
        return "local" + subnetNumber + "host" + netNumber;
    }
}
