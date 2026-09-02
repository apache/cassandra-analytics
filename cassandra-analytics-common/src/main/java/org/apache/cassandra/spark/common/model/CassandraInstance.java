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

package org.apache.cassandra.spark.common.model;

import org.apache.cassandra.spark.data.model.TokenOwner;
import org.jetbrains.annotations.Nullable;

public interface CassandraInstance extends TokenOwner
{
    /**
     * @return ID string that can uniquely identify a cluster; the return value is nullable
     */
    @Nullable String clusterId();

    String nodeName();

    String datacenter();

    /**
     * IP address string (w/o port) of a Cassandra instance.
     * Prefer to use {@link #ipAddressWithPort} as instance identifier,
     * unless knowing the compared is IP address without port for sure.
     */
    String ipAddress();

    /**
     * Equivalent to EndpointWithPort in Cassandra
     * @return ip address with port string in the format, "ip:port"
     */
    String ipAddressWithPort();

    /**
     * @return state of the node
     */
    NodeState nodeState();

    /**
     * @return status of the node
     */
    NodeStatus nodeStatus();

    /**
     * Returns the identifier of the specific Cassandra instance that a shared Sidecar endpoint should route
     * requests to, or {@code null} when not configured.
     *
     * <p>This is only meaningful when a single Sidecar endpoint (for example, one fronted by a load balancer)
     * fronts more than one Cassandra instance: the id disambiguates which local instance a request targets,
     * since the endpoint alone (hostname/Host header) cannot. When {@code null}, callers fall back to a
     * job-level default, if any.
     *
     * @return the per-instance Sidecar routing id, or {@code null} when not set
     */
    default @Nullable Integer sidecarInstanceId()
    {
        return null;
    }
}
