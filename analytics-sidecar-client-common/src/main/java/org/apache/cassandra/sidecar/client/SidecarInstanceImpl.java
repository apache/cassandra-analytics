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

import java.util.Objects;

/**
 * A simple implementation of the {@link SidecarInstance} interface
 */
public class SidecarInstanceImpl implements SidecarInstance
{
    protected int port;
    protected String hostname;
    protected Integer instanceId;

    /**
     * Constructs a new Sidecar instance with the given {@code port} and {@code hostname} and no
     * per-instance identifier (requests fall back to the job-level {@code instanceId}, if any).
     *
     * @param hostname the host name where Sidecar is running
     * @param port     the port where Sidecar is running
     */
    public SidecarInstanceImpl(String hostname, int port)
    {
        this(hostname, port, null);
    }

    /**
     * Constructs a new Sidecar instance with the given {@code hostname}, {@code port} and per-instance
     * {@code instanceId}.
     *
     * @param hostname   the host name where Sidecar is running
     * @param port       the port where Sidecar is running
     * @param instanceId the identifier of the Cassandra instance that requests sent to this Sidecar
     *                   endpoint should be routed to, or {@code null} to fall back to the job-level
     *                   {@code instanceId}
     */
    public SidecarInstanceImpl(String hostname, int port, Integer instanceId)
    {
        if (port < 1 || port > 65535)
        {
            throw new IllegalArgumentException(String.format("Invalid port number for the Sidecar service: %d",
                                                             port));
        }
        if (instanceId != null && instanceId < 0)
        {
            throw new IllegalArgumentException(String.format("Invalid instanceId for the Sidecar service: %d",
                                                             instanceId));
        }
        this.port = port;
        this.hostname = Objects.requireNonNull(hostname, "The Sidecar hostname must be non-null");
        this.instanceId = instanceId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int port()
    {
        return port;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String hostname()
    {
        return hostname;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer instanceId()
    {
        return instanceId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        SidecarInstanceImpl that = (SidecarInstanceImpl) o;
        return port == that.port && Objects.equals(hostname, that.hostname) && Objects.equals(instanceId, that.instanceId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(port, hostname, instanceId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "SidecarInstanceImpl{" +
               "port=" + port +
               ", hostname='" + hostname + '\'' +
               ", instanceId=" + instanceId +
               '}';
    }
}
