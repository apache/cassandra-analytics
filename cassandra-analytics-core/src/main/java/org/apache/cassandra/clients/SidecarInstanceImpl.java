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

package org.apache.cassandra.clients;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.sidecar.client.SidecarInstance;

/**
 * A simple implementation of the {@link SidecarInstance} interface
 */
public class SidecarInstanceImpl implements Serializable, SidecarInstance
{
    private static final long serialVersionUID = -8650006905764842232L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarInstanceImpl.class);

    private int port;
    private String hostname;

    /**
     * Constructs a new Sidecar instance with the given {@code port} and {@code hostname}
     *
     * @param hostname the host name where Sidecar is running
     * @param port     the port where Sidecar is running
     */
    public SidecarInstanceImpl(String hostname, int port)
    {
        Preconditions.checkArgument(0 < port && port <= 65535,
                                    "The Sidecar port number must be in the range 1-65535: %s", port);
        this.port = port;
        this.hostname = Objects.requireNonNull(hostname, "The Sidecar hostname must be non-null");
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
    public String toString()
    {
        return String.format("SidecarInstanceImpl{hostname='%s', port=%d}", hostname, port);
    }

    // JDK Serialization

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        LOGGER.debug("Falling back to JDK deserialization");
        hostname = in.readUTF();
        port = in.readInt();
    }

    private void writeObject(ObjectOutputStream out) throws IOException, ClassNotFoundException
    {
        LOGGER.debug("Falling back to JDK serialization");
        out.writeUTF(hostname);
        out.writeInt(port);
    }

    // Kryo Serialization

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<SidecarInstanceImpl>
    {
        @Override
        public void write(Kryo kryo, Output out, SidecarInstanceImpl sidecarInstance)
        {
            out.writeString(sidecarInstance.hostname);
            out.writeInt(sidecarInstance.port);
        }

        @Override
        public SidecarInstanceImpl read(Kryo kryo, Input input, Class<SidecarInstanceImpl> type)
        {
            return new SidecarInstanceImpl(input.readString(), input.readInt());
        }
    }
}
