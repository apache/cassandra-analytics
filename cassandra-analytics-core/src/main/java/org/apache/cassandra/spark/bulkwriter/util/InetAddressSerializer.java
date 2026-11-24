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

package org.apache.cassandra.spark.bulkwriter.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Kryo serializer for the {@link InetAddress} values of {@code inet} columns, which Spark shuffles before they are
 * written. Kryo's default serializer skips the transient field that holds the address and creates the copy with the
 * no-argument constructor, so every address would arrive as {@code 0.0.0.0} or {@code ::}.
 * <p>
 * Only the raw address is kept, which is all that Cassandra stores for an {@code inet} value.
 */
public class InetAddressSerializer extends Serializer<InetAddress>
{
    @Override
    public void write(Kryo kryo, Output output, InetAddress address)
    {
        byte[] bytes = address.getAddress();
        output.writeByte(bytes.length);
        output.writeBytes(bytes);
    }

    @Override
    public InetAddress read(Kryo kryo, Input input, Class<InetAddress> type)
    {
        byte[] bytes = input.readBytes(input.readByte());
        try
        {
            return InetAddress.getByAddress(bytes);
        }
        catch (UnknownHostException exception)
        {
            throw new KryoException("Invalid inet address of " + bytes.length + " bytes", exception);
        }
    }
}
