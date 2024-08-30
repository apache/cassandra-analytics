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

    public RingInstance(RingEntry ringEntry)
    {
        this.ringEntry = ringEntry;
    }

    public RingInstance(ReplicaMetadata replica)
    {
        this.ringEntry = new RingEntry.Builder()
                         .fqdn(replica.fqdn())
                         .address(replica.address())
                         .port(replica.port())
                         .datacenter(replica.datacenter())
                         .state(replica.state())
                         .status(replica.status())
                         .build();
    }

    // Used only in tests
    @Override
    public String token()
    {
        return ringEntry.token();
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

    /**
     * Custom equality that compares the token, fully qualified domain name, the port, and the datacenter
     *
     * Note that node state and status are not part of the calculation.
     *
     * @param other the other instance
     * @return true if both instances are equal, false otherwise
     */
    @Override
    public boolean equals(@Nullable Object other)
    {
        if (other == null || !(other instanceof RingInstance))
        {
            return false;
        }
        final RingInstance that = (RingInstance) other;
        return Objects.equals(ringEntry.token(), that.ringEntry.token())
               && Objects.equals(ringEntry.fqdn(), that.ringEntry.fqdn())
               && Objects.equals(ringEntry.address(), that.ringEntry.address())
               && ringEntry.port() == that.ringEntry.port()
               && Objects.equals(ringEntry.datacenter(), that.ringEntry.datacenter());
    }

    /**
     * Custom hashCode that compares the token, fully qualified domain name, the port, and the datacenter
     *
     * Note that node state and status are not part of the calculation.
     *
     * @return The hashcode of this instance based on the important fields
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(ringEntry.token(), ringEntry.fqdn(), ringEntry.port(), ringEntry.datacenter(), ringEntry.address());
    }

    @Override
    public String toString()
    {
        return ringEntry.toString();
    }

    public RingEntry ringInstance()
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
    }
}
