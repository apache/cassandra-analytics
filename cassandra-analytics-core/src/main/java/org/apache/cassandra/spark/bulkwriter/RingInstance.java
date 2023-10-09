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

import org.apache.cassandra.sidecar.common.data.RingEntry;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse.ReplicaMetadata;
import org.apache.cassandra.spark.common.model.CassandraInstance;
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
    public String getToken()
    {
        return ringEntry.token();
    }

    @Override
    public String getNodeName()
    {
        return ringEntry.fqdn();
    }

    @Override
    public String getDataCenter()
    {
        return ringEntry.datacenter();
    }

    @Override
    public String getIpAddress()
    {
        return ringEntry.address();
    }

    /**
     * Custom equality that compares the token, fully qualified domain name, the port, and the datacenter
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

    public RingEntry getRingInstance()
    {
        return ringEntry;
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
        out.writeUTF(ringEntry.address());
        out.writeInt(ringEntry.port());
        out.writeUTF(ringEntry.datacenter());
        out.writeUTF(ringEntry.load());
        out.writeUTF(ringEntry.token());
        out.writeUTF(ringEntry.fqdn());
        out.writeUTF(ringEntry.rack());
        out.writeUTF(ringEntry.hostId());
        out.writeUTF(ringEntry.status());
        out.writeUTF(ringEntry.state());
        out.writeUTF(ringEntry.owns());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        String address = in.readUTF();
        int port = in.readInt();
        String datacenter = in.readUTF();
        String load = in.readUTF();
        String token = in.readUTF();
        String fqdn = in.readUTF();
        String rack = in.readUTF();
        String hostId = in.readUTF();
        String status = in.readUTF();
        String state = in.readUTF();
        String owns = in.readUTF();
        ringEntry = new RingEntry.Builder().datacenter(datacenter)
                                           .address(address)
                                           .port(port)
                                           .rack(rack)
                                           .status(status)
                                           .state(state)
                                           .load(load)
                                           .owns(owns)
                                           .token(token)
                                           .fqdn(fqdn)
                                           .hostId(hostId)
                                           .build();
    }
}
