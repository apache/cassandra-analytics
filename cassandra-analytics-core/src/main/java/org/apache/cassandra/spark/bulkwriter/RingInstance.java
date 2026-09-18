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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

import o.a.c.sidecar.client.shaded.common.response.TokenRangeReplicasResponse.ReplicaMetadata;
import o.a.c.sidecar.client.shaded.common.response.data.RingEntry;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.common.model.NodeState;
import org.apache.cassandra.spark.common.model.NodeStatus;
import org.jetbrains.annotations.Nullable;

public class RingInstance implements CassandraInstance, Serializable
{
    private static final long serialVersionUID = 4399143234683369652L;
    private RingEntry ringEntry;
    private @Nullable String clusterId;
    private @Nullable Integer sidecarInstanceId;

    public RingInstance(ReplicaMetadata replica, @Nullable String clusterId)
    {
        this(replica, clusterId, null);
    }

    /**
     * @param sidecarInstanceId the id of the Cassandra instance that a shared Sidecar endpoint fronting this
     *                          instance should route requests to, or {@code null} when not configured for this
     *                          instance (see {@link CassandraInstance#sidecarInstanceId()})
     */
    public RingInstance(ReplicaMetadata replica, @Nullable String clusterId, @Nullable Integer sidecarInstanceId)
    {
        this.clusterId = clusterId;
        this.sidecarInstanceId = sidecarInstanceId;
        this.ringEntry = new RingEntry.Builder()
                         .fqdn(replica.fqdn())
                         .address(replica.address())
                         .port(replica.port())
                         .datacenter(replica.datacenter())
                         .state(replica.state())
                         .status(replica.status())
                         .build();
    }

    @VisibleForTesting
    public RingInstance(RingEntry ringEntry)
    {
        this(ringEntry, null);
    }

    @VisibleForTesting
    public RingInstance(RingEntry ringEntry, @Nullable String clusterId)
    {
        this(ringEntry, clusterId, null);
    }

    @VisibleForTesting
    public RingInstance(RingEntry ringEntry, @Nullable String clusterId, @Nullable Integer sidecarInstanceId)
    {
        this.clusterId = clusterId;
        this.sidecarInstanceId = sidecarInstanceId;
        this.ringEntry = ringEntry;
    }

    @VisibleForTesting
    public RingInstance(ReplicaMetadata replica)
    {
        this(replica, null);
    }

    // Used only in tests
    @Override
    public String token()
    {
        return ringEntry.token();
    }

    @Override
    @Nullable
    public String clusterId()
    {
        return clusterId;
    }

    @Override
    public String nodeName()
    {
        return ringEntry.fqdn();
    }

    @Override
    public String datacenter()
    {
        return ringEntry.datacenter();
    }

    @Override
    public String ipAddress()
    {
        return ringEntry.address();
    }

    @Override
    public String ipAddressWithPort()
    {
        return ringEntry.address() + ':' + ringEntry.port();
    }

    @Override
    public NodeState nodeState()
    {
        return NodeState.fromNameIgnoreCase(ringEntry.state());
    }

    @Override
    public NodeStatus nodeStatus()
    {
        return NodeStatus.fromNameIgnoreCase(ringEntry.status());
    }

    @Override
    @Nullable
    public Integer sidecarInstanceId()
    {
        return sidecarInstanceId;
    }

    /**
     * Custom equality that compares the token, fully qualified domain name, the rack, the port, the datacenter
     * and the clusterId
     *
     * Note that node state, status, IP address and sidecarInstanceId are not part of the calculation. The IP
     * address is excluded because a node can come back with a different IP address (e.g. a pod replacement in
     * Kubernetes) while remaining the same logical instance. sidecarInstanceId is excluded because it is routing
     * metadata derived from configuration, not part of the instance's identity.
     *
     * @param other the other instance
     * @return true if both instances are equal, false otherwise
     */
    @Override
    public boolean equals(@Nullable Object other)
    {
        if (this == other)
        {
            return true;
        }

        if (other == null || getClass() != other.getClass())
        {
            return false;
        }
        final RingInstance that = (RingInstance) other;
        return Objects.equals(clusterId, that.clusterId)
               && Objects.equals(ringEntry.token(), that.ringEntry.token())
               && Objects.equals(ringEntry.fqdn(), that.ringEntry.fqdn())
               && Objects.equals(ringEntry.rack(), that.ringEntry.rack())
               && ringEntry.port() == that.ringEntry.port()
               && Objects.equals(ringEntry.datacenter(), that.ringEntry.datacenter());
    }

    /**
     * Custom hashCode that hashes the token, fully qualified domain name, the rack, the port, the datacenter
     * and the clusterId
     *
     * Note that node state, status and IP address are not part of the calculation.
     *
     * @return The hashcode of this instance based on the important fields
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(clusterId, ringEntry.token(), ringEntry.fqdn(), ringEntry.rack(), ringEntry.port(), ringEntry.datacenter());
    }

    @Override
    public String toString()
    {
        return "RingInstance{cluster='" + clusterId + "', sidecarInstanceId=" + sidecarInstanceId + ", " + ringEntry.toString() + '}';
    }

    public RingEntry ringEntry()
    {
        return ringEntry;
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
        out.writeUTF(ringEntry.address());
        out.writeInt(ringEntry.port());
        out.writeUTF(ringEntry.datacenter());
        out.writeUTF(ringEntry.fqdn());
        out.writeUTF(ringEntry.status());
        out.writeUTF(ringEntry.state());
        // Nullable fields serialized with writeObject
        out.writeObject(ringEntry.token());
        out.writeObject(ringEntry.rack());
        out.writeObject(ringEntry.hostId());
        out.writeObject(ringEntry.load());
        out.writeObject(ringEntry.owns());
        out.writeObject(clusterId);
        out.writeObject(sidecarInstanceId);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        String address = in.readUTF();
        int port = in.readInt();
        String datacenter = in.readUTF();
        String fqdn = in.readUTF();
        String status = in.readUTF();
        String state = in.readUTF();
        // Nullable fields deserialized with readObject
        String token = (String) in.readObject();
        String rack = (String) in.readObject();
        String hostId = (String) in.readObject();
        String load = (String) in.readObject();
        String owns = (String) in.readObject();
        String clusterId = (String) in.readObject();
        Integer sidecarInstanceId = (Integer) in.readObject();
        ringEntry = new RingEntry.Builder().datacenter(datacenter)
                                           .address(address)
                                           .port(port)
                                           .status(status)
                                           .state(state)
                                           .token(token)
                                           .fqdn(fqdn)
                                           .rack(rack)
                                           .hostId(hostId)
                                           .load(load)
                                           .owns(owns)
                                           .build();
        this.clusterId = clusterId;
        this.sidecarInstanceId = sidecarInstanceId;
    }
}
