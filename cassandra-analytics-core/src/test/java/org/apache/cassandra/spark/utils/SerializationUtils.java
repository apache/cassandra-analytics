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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * A test helper class for serialization/deserialization methods
 */
public final class SerializationUtils
{
    private static final Kryo KRYO = new Kryo();

    private SerializationUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static void register(Class<?> type, Serializer<?> serializer)
    {
        KRYO.register(type, serializer);
    }

    public static byte[] serialize(Object object)
    {
        try
        {
            ByteArrayOutputStream arOut = new ByteArrayOutputStream(512);
            try (ObjectOutputStream out = new ObjectOutputStream(arOut))
            {
                out.writeObject(object);
            }
            return arOut.toByteArray();
        }
        catch (IOException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    public static <T> T deserialize(byte[] bytes, Class<T> type)
    {
        ObjectInputStream in;
        try
        {
            in = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return type.cast(in.readObject());
        }
        catch (IOException | ClassNotFoundException exception)
        {
            throw new RuntimeException(exception);
        }
    }

    public static Output kryoSerialize(Object object)
    {
        try (Output out = new Output(1024, -1))
        {
            KRYO.writeObject(out, object);
            return out;
        }
    }

    public static <T> T kryoDeserialize(Output out, Class<T> type)
    {
        try (Input in = new Input(out.getBuffer(), 0, out.position()))
        {
            return KRYO.readObject(in, type);
        }
    }
}
