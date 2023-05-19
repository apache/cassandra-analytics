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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.ObjectMap;

/**
 * Lifted from Kryo 4.0 to fix issues with ObjectInputStream not using the correct class loader
 * See https://github.com/EsotericSoftware/kryo/blob/19a6b5edee7125fbaf54c64084a8d0e13509920b/src/com/esotericsoftware/kryo/serializers/JavaSerializer.java
 */
// noinspection unchecked
public class SbwJavaSerializer extends Serializer
{
    public void write(Kryo kryo, Output output, Object object)
    {
        try
        {
            ObjectMap graphContext = kryo.getGraphContext();
            ObjectOutputStream objectStream = (ObjectOutputStream) graphContext.get(this);
            if (objectStream == null)
            {
                objectStream = new ObjectOutputStream(output);
                graphContext.put(this, objectStream);
            }
            objectStream.writeObject(object);
            objectStream.flush();
        }
        catch (Exception exception)
        {
            throw new KryoException("Error during Java serialization.", exception);
        }
    }

    public Object read(Kryo kryo, Input input, Class type)
    {
        try
        {
            ObjectMap graphContext = kryo.getGraphContext();
            ObjectInputStream objectStream = (ObjectInputStream) graphContext.get(this);
            if (objectStream == null)
            {
                objectStream = new ObjectInputStreamWithKryoClassLoader(input, kryo);
                graphContext.put(this, objectStream);
            }
            return objectStream.readObject();
        }
        catch (Exception exception)
        {
            throw new KryoException("Error during Java deserialization.", exception);
        }
    }

    /**
     * ${@link ObjectInputStream} uses the last user-defined ${@link ClassLoader} which may not be the correct one.
     * This is a known Java issue and is often solved by using a specific class loader.
     * See:
     * https://github.com/apache/spark/blob/v1.6.3/streaming/src/main/scala/org/apache/spark/streaming/Checkpoint.scala#L154
     * https://issues.apache.org/jira/browse/GROOVY-1627
     */
    private static class ObjectInputStreamWithKryoClassLoader extends ObjectInputStream
    {
        private final ClassLoader loader;

        ObjectInputStreamWithKryoClassLoader(InputStream in, Kryo kryo) throws IOException
        {
            super(in);
            this.loader = kryo.getClassLoader();
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
        {
            try
            {
                return Class.forName(desc.getName(), false, loader);
            }
            catch (ClassNotFoundException exception)
            {
                throw new RuntimeException("Class not found: " + desc.getName(), exception);
            }
        }
    }
}
