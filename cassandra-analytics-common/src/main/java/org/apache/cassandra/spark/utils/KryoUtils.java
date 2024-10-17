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

package org.apache.cassandra.spark.utils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.bridge.TokenRange;
import org.jetbrains.annotations.Nullable;

public class KryoUtils
{
    private KryoUtils()
    {

    }

    public static <T> byte[] serializeToBytes(Kryo kryo,
                                              T obj,
                                              Serializer<T> serializer)
    {
        try (Output out = serialize(kryo, obj, serializer))
        {
            return out.getBuffer();
        }
    }

    public static <T> Output serialize(Kryo kryo,
                                       Object obj,
                                       Serializer<T> serializer)
    {
        try (Output out = new Output(1024, -1))
        {
            kryo.writeObject(out, obj, serializer);
            return out;
        }
    }

    public static <T> byte[] serializeToBytes(Kryo kryo,
                                              T obj)
    {
        try (Output out = serialize(kryo, obj))
        {
            return out.getBuffer();
        }
    }

    public static Output serialize(Kryo kryo,
                                   Object obj)
    {
        try (Output out = new Output(1024, -1))
        {
            kryo.writeObject(out, obj);
            return out;
        }
    }

    public static <T> T deserialize(Kryo kryo,
                                    byte[] ar,
                                    Class<T> type)
    {
        try (Input in = new Input(ar, 0, ar.length))
        {
            return kryo.readObject(in, type);
        }
    }

    public static <T> T deserialize(Kryo kryo,
                                    Output out,
                                    Class<T> type)
    {
        try (Input in = new Input(out.getBuffer(), 0, (int) out.total()))
        {
            return kryo.readObject(in, type);
        }
    }

    public static <T> T deserialize(Kryo kryo,
                                    ByteBuffer buf,
                                    Class<T> type,
                                    Serializer<T> serializer)
    {
        byte[] ar = new byte[buf.remaining()];
        buf.get(ar);
        return deserialize(kryo, ar, type, serializer);
    }

    public static <T> T deserialize(Kryo kryo,
                                    byte[] ar,
                                    Class<T> type,
                                    Serializer<T> serializer)
    {
        try (Input in = new Input(ar, 0, ar.length))
        {
            return kryo.readObject(in, type, serializer);
        }
    }

    public static void writeRange(Output out, @Nullable TokenRange range)
    {
        if (range != null)
        {
            KryoUtils.writeBigInteger(out, range.lowerEndpoint());
            KryoUtils.writeBigInteger(out, range.upperEndpoint());
        }
        else
        {
            out.writeByte(-1);
        }
    }

    @Nullable
    public static TokenRange readRange(Input in)
    {
        BigInteger lower = readBigInteger(in);
        if (lower != null)
        {
            return TokenRange.openClosed(lower, Objects.requireNonNull(readBigInteger(in)));
        }
        return null;
    }

    public static void writeBigInteger(Output out, BigInteger bi)
    {
        byte[] ar = bi.toByteArray();
        out.writeByte(ar.length); // Murmur3 is max 8-bytes, RandomPartitioner is max 16-bytes
        out.writeBytes(ar);
    }

    @Nullable
    public static BigInteger readBigInteger(Input in)
    {
        int len = in.readByte();
        if (len > 0)
        {
            byte[] ar = new byte[len];
            in.readBytes(ar);
            return new BigInteger(ar);
        }
        return null;
    }
}
