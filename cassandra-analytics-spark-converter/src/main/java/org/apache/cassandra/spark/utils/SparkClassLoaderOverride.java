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

import java.io.ObjectInputStream;
import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.data.CqlTable;
import org.apache.spark.serializer.JavaDeserializationStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This class is an {@link AutoCloseable} wrapper that allows to temporarily substitute the instance of
 * {@link ClassLoader} in use by a {@link ObjectInputStream} constructed by Spark for performing JDK deserialization.
 * Such substitution is required in order to resolve types of Cassandra-version-dependent objects,
 * specifically those defined under {@code org.apache.cassandra.spark.data.types} and used by the {@link CqlTable}.
 */
public class SparkClassLoaderOverride implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkClassLoaderOverride.class);

    @NotNull  private final ObjectInputStream in;
    @Nullable private final ClassLoader loader;

    public SparkClassLoaderOverride(@NotNull ObjectInputStream in, @NotNull ClassLoader loader)
    {
        this.in = in;
        this.loader = swapClassLoader(in, loader);
    }

    @Nullable
    private static ClassLoader swapClassLoader(@NotNull ObjectInputStream in, @NotNull ClassLoader loader)
    {
        try
        {
            Field outerField = in.getClass().getDeclaredField("$outer");
            outerField.setAccessible(true);
            JavaDeserializationStream outerValue = (JavaDeserializationStream) outerField.get(in);
            Field loaderField = outerValue.getClass().getDeclaredField("org$apache$spark$serializer$JavaDeserializationStream$$loader");
            loaderField.setAccessible(true);
            ClassLoader loaderValue = (ClassLoader) loaderField.get(outerValue);
            loaderField.set(outerValue, loader);
            return loaderValue;
        }
        catch (NullPointerException | NoSuchFieldException | IllegalAccessException | ClassCastException exception)
        {
            // Ignore all of the above exceptions: if any of them occurs, we are either not deserializing
            // from a Spark input stream, or Spark input stream's internal implementation has changed -
            // in either case we should abort the substitution and let deserialization fail
            // when resolving types of Cassandra-version-specific objects
            LOGGER.warn("Cannot override class loader in a Spark object input stream", exception);
            return null;
        }
    }

    @Override
    public void close()
    {
        if (loader != null)
        {
            swapClassLoader(in, loader);
        }
    }
}
